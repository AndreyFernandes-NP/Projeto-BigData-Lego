import os
import sys
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.__config__ import PATHS
from src.data_loader import load_all_datasets, load_saved_datasets, save_datasets_as_parquet
from src.spark_session import create_spark_session

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

def analyze_theme_production(spark: SparkSession) -> None:
    """Compara os temas com mais conjuntos lançados em períodos de até 10 anos."""

    sets = spark.read.parquet(str(DATA_DIR / "sets.parquet")).select("set_num", "year", "theme_id")
    themes = spark.read.parquet(str(DATA_DIR / "themes.parquet")).select("id", F.col("name").alias("theme_name"))

    dated_sets = sets.filter(F.col("year") > 1989 ).filter(F.col("year") < 2027)
    latest_year_row = dated_sets.agg(F.max("year").alias("latest_year")).first()

    if latest_year_row is None:
        raise ValueError("Não há conjuntos com ano de lançamento a partir de 1989.")

    latest_year = latest_year_row["latest_year"]

    period_start = (F.floor((F.col("year") - F.lit(1990)) / F.lit(10)) * F.lit(10)+ F.lit(1990))
    theme_counts = (
        dated_sets.join(themes, dated_sets.theme_id == themes.id, "left")
        .withColumn(
            "theme_name",
            F.coalesce(F.col("theme_name"), F.lit("Tema não identificado")),
        )
        .withColumn("period_start", period_start.cast("int"))
        .withColumn(
            "period_end",
            F.least(F.col("period_start") + F.lit(9), F.lit(int(latest_year))),
        )
        .withColumn(
            "period",
            F.concat_ws(
                "-",
                F.col("period_start").cast("string"),
                F.col("period_end").cast("string"),
            ),
        )
        .groupBy("period", "theme_name")
        .agg(F.count_distinct("set_num").alias("sets_produced"))
    )

    ranking = Window.partitionBy("period").orderBy(F.desc("sets_produced"), F.asc("theme_name"))

    top_themes = (
        theme_counts.withColumn("rank", F.row_number().over(ranking))
        .filter(F.col("rank") <= 5)
        .orderBy("period", F.col("rank"))
        .toPandas()
    )

    periods = top_themes["period"].drop_duplicates().tolist()

    print("\n[Spark] Tema com mais conjuntos por período:")
    for period in periods:
        leader = top_themes.loc[top_themes["period"] == period].iloc[0]
        print(
            f"  {period}: {leader['theme_name']} "
            f"({leader['sets_produced']} conjuntos)"
        )

    figure, axes = plt.subplots(
        nrows=len(periods),
        ncols=1,
        figsize=(10, max(3.5, 3.2 * len(periods))),
        squeeze=False,
    )

    for axis, period in zip(axes.flat, periods):
        period_top = top_themes.loc[top_themes["period"] == period].sort_values("sets_produced")
        axis.barh(period_top["theme_name"], period_top["sets_produced"])
        axis.set_title(f"Temas com mais conjuntos - {period}")
        axis.set_xlabel("Quantidade de conjuntos")
        axis.set_ylabel("Tema")

    figure.suptitle("Produção de conjuntos LEGO por tema e período")
    figure.tight_layout(rect=(0, 0, 1, 0.97))
    plt.show()

def analyze_theme_materials(spark: SparkSession) -> None:
    """Compara a quantidade de peças por tema geral entre 1990 e 2026."""
    sets = (
        spark.read.parquet(str(DATA_DIR / "sets.parquet"))
        .filter(F.col("year").between(1990, 2026))
        .select("set_num", "year", "theme_id", "num_parts")
    )

    themes = spark.read.parquet(str(DATA_DIR / "themes.parquet")).select(
        "id", "name", "parent_id"
    )

    latest_year_row = sets.agg(F.max("year").alias("latest_year")).first()
    if latest_year_row is None:
        raise ValueError("Não há conjuntos entre 1990 e 2026 para analisar.")
    
    latest_year = latest_year_row["latest_year"]
    if latest_year is None:
        raise ValueError("Não há conjuntos entre 1990 e 2026 para analisar.")

    theme_hierarchy = themes.select(
        F.col("id").alias("theme_id"),
        F.col("name").alias("general_theme"),
        F.col("parent_id").alias("next_parent_id"),
    )

    for _ in range(themes.count() + 1):
        unresolved = theme_hierarchy.filter(F.col("next_parent_id").isNotNull())
        if unresolved.limit(1).count() == 0:
            break

        parent_themes = themes.alias("parent")
        expanded = unresolved.alias("child").join(
            parent_themes,
            F.col("child.next_parent_id") == F.col("parent.id"),
            "left",
        )
        if expanded.filter(F.col("parent.id").isNull()).limit(1).count():
            raise ValueError("A hierarquia de temas contém um tema pai inexistente.")

        theme_hierarchy = theme_hierarchy.filter(
            F.col("next_parent_id").isNull()
        ).unionByName(
            expanded.select(
                F.col("child.theme_id").alias("theme_id"),
                F.col("parent.name").alias("general_theme"),
                F.col("parent.parent_id").alias("next_parent_id"),
            )
        )
    else:
        raise ValueError("A hierarquia de temas contém um ciclo.")

    sets_with_general_theme = (
        sets.join(theme_hierarchy.select("theme_id", "general_theme"), "theme_id", "left")
        .withColumn(
            "general_theme",
            F.coalesce(F.col("general_theme"), F.lit("Tema não identificado")),
        )
    )

    period_start = (
        F.floor((F.col("year") - F.lit(1990)) / F.lit(10)) * F.lit(10)
        + F.lit(1990)
    )

    sets_with_period = (
        sets_with_general_theme.withColumn("period_start", period_start.cast("int"))
        .withColumn(
            "period_end",
            F.least(F.col("period_start") + F.lit(9), F.lit(int(latest_year))),
        )
        .withColumn(
            "period",
            F.concat_ws(
                "-",
                F.col("period_start").cast("string"),
                F.col("period_end").cast("string"),
            ),
        )
    )

    overall_top = (
        sets_with_general_theme.groupBy("general_theme")
        .agg(F.sum("num_parts").alias("total_parts"))
        .orderBy(F.desc("total_parts"), F.asc("general_theme"))
        .limit(10)
        .toPandas()
    )
    period_totals = sets_with_period.groupBy(
        "period", "general_theme"
    ).agg(F.sum("num_parts").alias("total_parts"))
    ranking = Window.partitionBy("period").orderBy(
        F.desc("total_parts"), F.asc("general_theme")
    )

    period_top = (
        period_totals.withColumn("rank", F.row_number().over(ranking))
        .filter(F.col("rank") <= 5)
        .orderBy("period", F.col("rank"))
        .toPandas()
    )

    periods = period_top["period"].drop_duplicates().tolist()

    print("\n[Spark] Temas gerais com mais peças contabilizadas (1990-2026):")
    for _, theme in overall_top.iterrows():
        print(f"  {theme['general_theme']}: {theme['total_parts']:,.0f} peças")

    print("\n[Spark] Tema geral com mais peças por período:")
    for period in periods:
        leader = period_top.loc[period_top["period"] == period].iloc[0]
        print(
            f"  {period}: {leader['general_theme']} "
            f"({leader['total_parts']:,.0f} peças)"
        )

    figure, axes = plt.subplots(
        nrows=len(periods) + 1,
        ncols=1,
        figsize=(11, max(5, 3.2 * (len(periods) + 1))),
        squeeze=False,
    )
    
    overall_axis = axes.flat[0]
    overall_axis.barh(
        overall_top.sort_values("total_parts")["general_theme"],
        overall_top.sort_values("total_parts")["total_parts"],
    )

    overall_axis.set_title("Temas gerais com mais peças - 1990-2026")
    overall_axis.set_xlabel("Soma de peças nos sets")
    overall_axis.set_ylabel("Tema geral")

    for axis, period in zip(axes.flat[1:], periods):
        period_data = period_top.loc[period_top["period"] == period].sort_values(
            "total_parts"
        )
        axis.barh(period_data["general_theme"], period_data["total_parts"])
        axis.set_title(f"Temas gerais com mais peças - {period}")
        axis.set_xlabel("Soma de peças nos sets")
        axis.set_ylabel("Tema geral")

    figure.suptitle("Produção de materiais LEGO por tema geral")
    figure.tight_layout(rect=(0, 0, 1, 0.97))
    plt.show()

def main() -> None:
    print("[Execução] Programa iniciado, executando carregamento de dados.")

    dataframes = load_data()
    for name, dataframe in dataframes.items():
        print(f"{name}: {dataframe.shape}")

    spark = create_spark_session()
    try:
        rdds = data_to_rdd(spark)

        if rdds:
            required_rdds = {"sets", "themes"}

            if not required_rdds.issubset(rdds):
                missing_rdds = ", ".join(sorted(required_rdds - set(rdds)))
                raise RuntimeError(f"Não foi possível executar a Análise, RDDs ausentes: {missing_rdds}")

            # Análise Andrey
            analyze_theme_production(spark)
            analyze_theme_materials(spark)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
