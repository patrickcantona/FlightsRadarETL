import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType, TimestampType
from extract import *
from clean_transform import *
from load import *
from FlightRadar24 import FlightRadar24API
from datetime import datetime
import subprocess


def main():
    print("Exécution du pipeline de flights_pipeline.py responsable de l'ETL des données de filghts et des résultats")
    
    # Initialisation de la session Spark
    spark = create_spark_session('Flights data processing')

    # Chemins des fichiers parquet
    flights_raw_parquet_files_path = "../proceded_data/flights/raw/"
    flights_clean_parquet_files_path = "../proceded_data/flights/clean/"
    flights_time_path = "../proceded_data/time/"

    # Définition du schéma des données de vol
    flights_schema = StructType([
        StructField("aircraft_code", StringType(), nullable=True),
        StructField("airline_iata", StringType(), nullable=True),
        StructField("airline_icao", StringType(), nullable=True),
        StructField("altitude", LongType(), nullable=True),
        StructField("callsign", StringType(), nullable=True),
        StructField("destination_airport_iata", StringType(), nullable=True),
        StructField("ground_speed", LongType(), nullable=True),
        StructField("heading", LongType(), nullable=True),
        StructField("icao_24bit", StringType(), nullable=True),
        StructField("id", StringType(), nullable=True),
        StructField("latitude", DoubleType(), nullable=True),
        StructField("longitude", DoubleType(), nullable=True),
        StructField("number", StringType(), nullable=True),
        StructField("on_ground", LongType(), nullable=True),
        StructField("origin_airport_iata", StringType(), nullable=True),
        StructField("registration", StringType(), nullable=True),
        StructField("squawk", StringType(), nullable=True),
        StructField("time", LongType(), nullable=True),
        StructField("vertical_speed", LongType(), nullable=True)
    ])

    # Colonnes nécessaires pour le nettoyage
    not_empty_column = 'id'
    time_column = 'time'

    # Date actuelle
    current_date = datetime.now()

    # Récupération des vols
    fr_api = FlightRadar24API()
    flights = fr_api.get_flights()

    # Création du DataFrame Spark à partir des données brutes
    raw_flights_df = create_dataframe_from_list(flights , spark)

    # Calcul des dates minimales et maximales du dernier données de vol
    min_max_time_flights_df = min_max_datetime(spark, raw_flights_df)
    
    # Stockage du dataframe min_max_time_flights_df
    save_dataframe_as_parquet_with_current_date(min_max_time_flights_df, flights_time_path, current_date, "append")
    # Stockage du dataframe brut raw_flights_df
    save_dataframe_as_parquet_with_current_date(raw_flights_df, flights_raw_parquet_files_path, current_date, "append")

    # Création du DataFrame Spark avec le schéma spécifié
    flights_df = create_dataframe_from_parquet_with_schema(flights_raw_parquet_files_path, spark, flights_schema)

    # Nettoyage des données à travers un pipeline de nettoyage
    cleaned_flights_df = run_spark_df_pipeline(flights_df, not_empty_column, not_empty_column, time_column)

    # Sauvegarde des données nettoyées
    save_dataframe_as_parquet_with_column_datetime(cleaned_flights_df, flights_clean_parquet_files_path, 'datetime', 'overwrite')
    print('Le pipeline flights_pipeline.py est terminé avec succès')

    # Arrêt de la session Spark
    spark.stop()

    # Exécution du script des résultats
    subprocess.run(['python', 'results.py'])


if __name__ == "__main__":
    main()
