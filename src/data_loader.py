"""
data_loader.py
--------------
Funções para carregar e armazenar os datasets LEGO.

Transforma os datasets em DataFrames do Pandas e os salva como arquivos Parquet para acesso rápido.
"""
import sys

import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.__config__ import PATHS

DATA_DIR = PATHS.data

DATASET_URLS = {
    "lego_sets": "https://raw.githubusercontent.com/KeithGalli/lego-analysis/master/datasets/lego_sets.csv",
    "themes": "https://cdn.rebrickable.com/media/downloads/themes.csv.gz",
    "sets": "https://cdn.rebrickable.com/media/downloads/sets.csv.gz",
    "parts": "https://cdn.rebrickable.com/media/downloads/parts.csv.gz",
    "inventories": "https://cdn.rebrickable.com/media/downloads/inventories.csv.gz",
    "inventory_parts": "https://cdn.rebrickable.com/media/downloads/inventory_parts.csv.gz",
    "colors": "https://cdn.rebrickable.com/media/downloads/colors.csv.gz",
    "part_categories": "https://cdn.rebrickable.com/media/downloads/part_categories.csv.gz",
    "elements": "https://cdn.rebrickable.com/media/downloads/elements.csv.gz",
    "part_relationships": "https://cdn.rebrickable.com/media/downloads/part_relationships.csv.gz",
    "minifigs": "https://cdn.rebrickable.com/media/downloads/minifigs.csv.gz",
    "inventory_minifigs": "https://cdn.rebrickable.com/media/downloads/inventory_minifigs.csv.gz",
    "inventory_sets": "https://cdn.rebrickable.com/media/downloads/inventory_sets.csv.gz",
}

def load_dataset(url: str) -> pd.DataFrame:
    """Carrega um CSV remoto, incluindo arquivos compactados em .gz."""
    return pd.read_csv(url)

def load_all_datasets() -> dict[str, pd.DataFrame]:
    """Baixa e retorna todos os datasets como DataFrames."""
    dataframes = {}

    for name, url in DATASET_URLS.items():
        try:
            dataframes[name] = load_dataset(url)
        except Exception as error:
            raise RuntimeError(f"Falha ao carregar o dataset '{name}' de {url}") from error

    return dataframes

def save_datasets_as_parquet(dataframes: dict[str, pd.DataFrame], output_dir: Path = DATA_DIR) -> None:
    """Salva cada DataFrame como Parquet na pasta de dados."""
    output_dir.mkdir(parents=True, exist_ok=True)

    for name, dataframe in dataframes.items():
        dataframe.to_parquet(output_dir / f"{name}.parquet", index=False)

def load_saved_datasets(data_dir: Path = DATA_DIR) -> dict[str, pd.DataFrame]:
    """Carrega os Parquets existentes e valida que o cache está completo."""
    
    expected_files = {f"{name}.parquet" for name in DATASET_URLS}
    parquet_files = {path.name for path in data_dir.glob("*.parquet")}
    missing_files = expected_files - parquet_files
    
    if missing_files:
        missing = ", ".join(sorted(missing_files))
        raise FileNotFoundError(
            f"O cache em {data_dir} está incompleto. Arquivos ausentes: {missing}"
        )

    return {
        path.stem: pd.read_parquet(path)
        for path in sorted(data_dir.glob("*.parquet"))
        if path.name in expected_files
    }
