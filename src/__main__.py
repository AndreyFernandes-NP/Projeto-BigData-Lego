import sys
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.__config__ import PATHS

DATA_DIR = PATHS.data

from src.data_loader import load_all_datasets, load_saved_datasets, save_datasets_as_parquet

def load_data() -> dict[str, "pd.DataFrame"]:
    """Carrega os datasets, seja de arquivos Parquet salvos ou baixando-os."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    has_files = any(path.is_file() for path in DATA_DIR.rglob("*"))

    if has_files:
        print(f"[Dados] Carregando datasets salvos em Parquet da pasta de dados.")
        dataframes = load_saved_datasets()

        print(f"[Dados] {len(dataframes)} datasets carregados dos Parquets.")
    else:
        print(f"[Dados] Nenhum dataset encontrado em Parquet. Baixando e salvando datasets.")

        dataframes = load_all_datasets()
        save_datasets_as_parquet(dataframes)

        print(f"[Dados] {len(dataframes)} datasets baixados e salvos.")

    return dataframes

def main() -> None:
    print(f"[Execução] Programa iniciado, executando carregamento de dados.")

    dataframes = load_data()
    for name, dataframe in dataframes.items():
        print(f"{name}: {dataframe.shape}")

if __name__ == "__main__":
    main()
