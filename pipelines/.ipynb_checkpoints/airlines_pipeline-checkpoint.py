import os
import shutil
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType, TimestampType
from extract import *
from clean_transform import *
from load import *
from FlightRadar24 import FlightRadar24API
from datetime import datetime


def main():
    print("Exécution du pipeline de airlines_pipeline.py responsable de l'ETL des données des airlines")
    fr_api = FlightRadar24API()
    spark = create_spark_session('Airlines data processing')

    # Chemins des fichiers parquet
    airlines_raw_parquet_files_path = "../proceded_data/airlines/raw/"
    airlines_clean_parquet_files_path = "../proceded_data/airlines/clean/"

    # Définition du schéma des données des compagnies aériennes
    airlines_schema = StructType([
        StructField("Code", StringType(), nullable=True),
        StructField("ICAO", StringType(), nullable=True),
        StructField("Name", StringType(), nullable=True)
    ])

    # Colonnes nécessaires pour le nettoyage
    not_empty_column = 'ICAO'
    time_column = None

    # Date actuelle
    current_date = datetime.now()

    # Récupération des compagnies aériennes
    airlines = fr_api.get_airlines()

    # Création du DataFrame Spark à partir des données brutes
    raw_airlines_df = create_dataframe_from_list(airlines, spark)

    # Sauvegarde des données brutes au format Parquet avec la date actuelle
    save_dataframe_as_parquet_with_current_date(raw_airlines_df, airlines_raw_parquet_files_path, current_date, "append")

    # Création du DataFrame Spark avec le schéma spécifié
    airlines_df = create_dataframe_from_parquet_with_schema(airlines_raw_parquet_files_path, spark, airlines_schema)

    # Nettoyage et transformation des données
    cleaned_airlines_df = run_spark_df_pipeline(airlines_df, not_empty_column, not_empty_column, time_column)

    # Suppression du répertoire existant des données nettoyées et création d'un nouveau répertoire
    shutil.rmtree(airlines_clean_parquet_files_path)
    os.makedirs(airlines_clean_parquet_files_path)

    # Sauvegarde des données nettoyées au format Parquet avec la date actuelle
    save_dataframe_as_parquet_with_current_date(cleaned_airlines_df, airlines_clean_parquet_files_path, current_date, "overwrite")

    print("Pipeline des compagnies aériennes terminé")
    
    # Arrêt de la session Spark
    spark.stop()


if __name__ == "__main__":
    main()
