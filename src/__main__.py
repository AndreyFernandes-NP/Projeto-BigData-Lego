import atexit
import os
import shutil
import subprocess
import sys
import tempfile
import time
import warnings
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

import pandas as pd
import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.__config__ import PATHS
from src.data_loader import load_all_datasets, load_saved_datasets, save_datasets_as_parquet

DATA_DIR = PATHS.data


def load_data() -> dict[str, pd.DataFrame]:
    """Carrega os datasets dos Parquets ou baixa-os quando ainda não existem."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    has_files = any(path.is_file() for path in DATA_DIR.rglob("*"))

    if has_files:
        print("[Dados] Carregando datasets salvos em Parquet da pasta de dados.")
        dataframes = load_saved_datasets()
        print(f"[Dados] {len(dataframes)} datasets carregados dos Parquets.")
    else:
        print("[Dados] Nenhum dataset encontrado em Parquet. Baixando e salvando datasets.")
        dataframes = load_all_datasets()
        save_datasets_as_parquet(dataframes)
        print(f"[Dados] {len(dataframes)} datasets baixados e salvos.")

    return dataframes


def data_to_rdd(spark: SparkSession):
    """Configura um RDD para cada arquivo Parquet na pasta de dados."""
    rdd_list = {}

    print("[Spark] Configurando RDDs a partir dos arquivos Parquet.")
    for filename in os.listdir(DATA_DIR):
        if filename.endswith(".parquet"):
            name = filename.replace(".parquet", "")
            file_path = DATA_DIR / filename

            try:
                spark_df = spark.read.parquet(str(file_path))
                rdd_list[name] = spark_df.rdd
                print(f"[Spark] RDD criado para o arquivo {filename}.")
            except Exception as error:
                print(f"[Erro] Falha ao ler o arquivo Parquet {filename}: {error}")

    print("[Spark] Configuração de RDDs concluída.")
    return rdd_list


@contextmanager
def create_spark_session(app_name: str = "Lego-EDA") -> Generator[SparkSession, None, None]:
    """Cria a sessão Spark e gerencia automaticamente Python, junction e limpeza."""
    # Eu admito que usei I.A pra criar isso pois a forma de estabelecer uma junction automática é MUITO complicada.

    python_executable = sys.executable
    environment_keys = ("PYSPARK_PYTHON", "PYSPARK_DRIVER_PYTHON", "SPARK_HOME")
    previous_environment = {key: os.environ.get(key) for key in environment_keys}
    previous_gateway = SparkContext._gateway

    temp_dir = None
    junction = None
    gateway_process = None
    cleanup_callback = None
    os.environ["PYSPARK_PYTHON"] = python_executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_executable

    try:
        if os.name == "nt":
            spark_home = Path(
                os.path.abspath(
                    os.path.expanduser(
                        os.environ.get(
                            "SPARK_HOME",
                            str(Path(pyspark.__file__).resolve().parent),
                        )
                    )
                )
            )

            if not str(spark_home).isascii():
                temp_candidates = (
                    Path(tempfile.gettempdir()),
                    Path(os.environ.get("SystemRoot", r"C:\Windows")) / "Temp",
                )
                for candidate in temp_candidates:
                    if not candidate.is_dir() or not str(candidate.resolve()).isascii():
                        continue
                    try:
                        temp_dir = Path(
                            tempfile.mkdtemp(prefix="spark-home-", dir=candidate)
                        )
                        break
                    except OSError:
                        continue

                if temp_dir is None:
                    raise RuntimeError(
                        "Não foi possível criar um diretório temporário com "
                        "caminho ASCII para iniciar o Spark."
                    )

                junction = temp_dir / "pyspark"
                result = subprocess.run(
                    [
                        "cmd.exe",
                        "/d",
                        "/c",
                        "mklink",
                        "/J",
                        str(junction),
                        str(spark_home),
                    ],
                    capture_output=True,
                    text=True,
                    check=False,
                )
                if result.returncode != 0:
                    shutil.rmtree(temp_dir)
                    temp_dir = None
                    junction = None
                    raise RuntimeError(
                        "Não foi possível criar o junction temporário do Spark: "
                        f"{result.stderr.strip() or result.stdout.strip()}"
                    )
                os.environ["SPARK_HOME"] = str(junction)

                def cleanup_junction() -> None:
                    if gateway_process is not None and gateway_process.poll() is None:
                        deadline = time.monotonic() + 10
                        while (
                            gateway_process.poll() is None
                            and time.monotonic() < deadline
                        ):
                            time.sleep(0.1)

                        if gateway_process.poll() is None:
                            warnings.warn(
                                "O gateway Spark ainda está ativo; o junction "
                                f"foi mantido em {junction}.",
                                RuntimeWarning,
                            )
                            return

                    try:
                        junction.rmdir()
                        shutil.rmtree(temp_dir)
                    except OSError as error:
                        warnings.warn(
                            f"Não foi possível remover o junction temporário "
                            f"{junction}: {error}",
                            RuntimeWarning,
                        )

                cleanup_callback = cleanup_junction
                atexit.register(cleanup_callback)

        spark = (
            SparkSession.builder.appName(app_name)
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.pyspark.python", python_executable)
            .config("spark.pyspark.driver.python", python_executable)
            .getOrCreate()
        )
        gateway = SparkContext._gateway
        if gateway is not None and gateway is not previous_gateway:
            gateway_process = gateway.proc

        try:
            yield spark
        finally:
            spark.stop()
    finally:
        if (
            cleanup_callback is not None
            and (gateway_process is None or gateway_process.poll() is not None)
        ):
            atexit.unregister(cleanup_callback)
            cleanup_callback()

        for key, value in previous_environment.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

def main() -> None:
    print("[Execução] Programa iniciado, executando carregamento de dados.")

    dataframes = load_data()
    for name, dataframe in dataframes.items():
        print(f"{name}: {dataframe.shape}")

    with create_spark_session() as spark:
        rdds = data_to_rdd(spark)

        if rdds:
            print("[Execução] RDDs criados a partir dos arquivos Parquet:")
            for name, rdd in rdds.items():
                print(f"{name}: {rdd.count()} registros")

if __name__ == "__main__":
    main()
