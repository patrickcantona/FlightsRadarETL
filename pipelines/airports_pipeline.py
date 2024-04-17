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
import json


def main():
    print("Exécution du pipeline de airports_pipeline.py responsable de l'ETL des données des airports")
    # Initialisation de l'API FlightRadar24
    fr_api = FlightRadar24API()
    
    # Initialisation de la session Spark
    spark = create_spark_session('Airports data processing')

    # Chemins des fichiers parquet pour les données brutes et nettoyées des aéroports
    airports_raw_parquet_files_path = "../proceded_data/airports/raw/"
    airports_clean_parquet_files_path = "../proceded_data/airports/clean/"

    # Schéma des données des aéroports
    airports_schema = StructType([
        StructField("altitude", LongType(), nullable=True),
        StructField("country", StringType(), nullable=True),
        StructField("iata", StringType(), nullable=True),
        StructField("icao", StringType(), nullable=True),
        StructField("latitude", DoubleType(), nullable=True),
        StructField("longitude", DoubleType(), nullable=True),
        StructField("name", StringType(), nullable=True)
    ])

    # Colonne nécessaire pour le nettoyage des données
    not_empty_column = 'iata'
    time_column = None

    # Date actuelle
    current_date = datetime.now()

    # Récupération des données des aéroports
    airports = fr_api.get_airports()

    # Création du DataFrame Spark à partir des données brutes
    raw_airports_df = create_dataframe_from_list(airports , spark)

    # Sauvegarde des données brutes au format parquet
    save_dataframe_as_parquet_with_current_date(raw_airports_df, airports_raw_parquet_files_path, current_date, "append")

    # Création du DataFrame Spark à partir des données brutes avec le schéma spécifié
    airports_df = create_dataframe_from_parquet_with_schema(airports_raw_parquet_files_path, spark , airports_schema)

    # Nettoyage des données des aéroports
    cleaned_airports_df = run_spark_df_pipeline(airports_df, not_empty_column, not_empty_column, time_column)

    # Suppression du dossier des données nettoyées des aéroports s'il existe, puis recréation
    shutil.rmtree(airports_clean_parquet_files_path)
    os.makedirs(airports_clean_parquet_files_path)

    # Sauvegarde des données nettoyées au format parquet
    save_dataframe_as_parquet_with_current_date(cleaned_airports_df, airports_clean_parquet_files_path, current_date, "overwrite")
    
    # Affichage du message de fin du traitement
    print("Pipeline des aéroports terminé")

    # Arrêt de la session Spark
    spark.stop()

if __name__ == "__main__":
    main()
