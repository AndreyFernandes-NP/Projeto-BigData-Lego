import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.data_loader import load_all_datasets

if __name__ == "__main__":
    dataframes = load_all_datasets()
    
    print(f"[Execução] {len(dataframes)} datasets carregados.")
    for name, dataframe in dataframes.items():
        print(f"{name}: {dataframe.shape}")
