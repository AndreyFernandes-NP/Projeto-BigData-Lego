import atexit
import os
import shutil
import subprocess
import sys
import tempfile
import time
import warnings
from pathlib import Path

import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession


def create_spark_session(app_name: str = "Lego-EDA") -> SparkSession:
    """Return the active Spark session or create one with required path handling."""
    active_session = SparkSession.getActiveSession()
    if active_session is not None:
        return active_session

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
        return spark
    except Exception:
        if junction is not None and temp_dir is not None:
            gateway = SparkContext._gateway
            gateway_process = (
                gateway.proc
                if gateway is not None and gateway is not previous_gateway
                else None
            )
            if gateway_process is None or gateway_process.poll() is not None:
                if cleanup_callback is not None:
                    atexit.unregister(cleanup_callback)
                    cleanup_callback()
        raise
    finally:
        for key, value in previous_environment.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
