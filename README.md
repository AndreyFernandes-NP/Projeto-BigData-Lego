# Projeto BigData Lego
Projeto envolvendo análise, exploração e manipulação de dados de um dataset de produções da Lego utilizando o Apache Spark, com o intuito de elaborar e desenvolver um notebook de EDA (Exploration Data Analysis), para a catalogação de padrões e produção de gráficos.

Feito para a matéria de I.A Big Data e Engenharia de Dados, da PUC-SP.

**Requisitos**
- Python 3.x
- Dependências instaladas [```pip install -r requirements.txt```]
- Ambiente virtual [recomendado]

## Estrutura do Repositório

```text
Projeto-Bigdata-Lego/
├── data/
|   ├── colors.parquet
|   ├── elements.parquet
|   ├── inventories.parquet
|   ├── inventory_minifigs.parquet
|   ├── inventory_parts.parquet
|   ├── inventory_sets.parquet
|   ├── lego_sets.parquet
|   ├── minifigs.parquet
|   ├── part_categories.parquet
|   ├── part_relationships.parquet
|   ├── parts.parquet
|   ├── sets.parquet
|   ├── themes.parquet
├── notebooks/
|   ├── EDA_lego.ipynb
├── src/
|   ├── __config__.py
|   ├── __init__.py
|   ├── __main__.py
|   ├── data_loader.py
├── requirements.txt
└── README.md
```

## Setup local

1. **Clone o repositório**
   ```bash
   git clone https://github.com/AndreyFernandes-NP/Projeto-BigData-Lego.git
   cd Projeto-BigData-Lego
   ```
2. **Crie um virtual environment**
   ```bash
    python -m venv .env
    ```
3. **Ative o ambiente virtual**
   ```bash
    .env/Scripts/Activate.ps1
    ```
3. **Instale as dependencias**
   ```bash
   pip install -r requirements.txt
    ```
4. **Execute o projeto**
   ```bash
   python -m src
    ```
****
