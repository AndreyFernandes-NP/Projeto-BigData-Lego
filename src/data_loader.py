"""
data_loader.py
--------------
Funções para carregar os datasets LEGO diretamente das URLs de origem.

"""

import pandas as pd

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
    """Baixa e retorna todos os datasets como DataFrames, sem salvá-los em disco."""
    dataframes = {}
    
    for name, url in DATASET_URLS.items():
        try:
            dataframes[name] = load_dataset(url)
        except Exception as error:
            raise RuntimeError(f"Falha ao carregar o dataset '{name}' de {url}") from error
        
    return dataframes
