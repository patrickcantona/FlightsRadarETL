import os
from datetime import datetime
import pandas as pd
import numpy as np
from extract import *
from load import * 
from clean_transform import *
from questions_sql_query import *
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, DoubleType, LongType, StringType, IntegerType, TimestampType
import json
from FlightRadar24 import FlightRadar24API



print("Génération des résultats à partir de flights_pipeline.py...")
spark = create_spark_session(app_name  = "ETL test results")

# Creer les dataframe à partir de parquet
flights_df_parquet = create_dataframe_from_parquet(spark, "../proceded_data/flights/clean")
airlines_df_parquet = create_dataframe_from_parquet(spark, "../proceded_data/airlines/clean")
airports_df_parquet = create_dataframe_from_parquet(spark, "../proceded_data/airports/clean")
zones_df_parquet = create_dataframe_from_json(spark, "../proceded_data/zones/clean")
time_df_parquet = create_dataframe_from_parquet(spark, "../proceded_data/time")
time_df_parquet.createOrReplaceTempView("flights_latest_update_time")

# Recuperer max datetime dans flights
max_datetime = flights_df_parquet.select(F.max('datetime')).collect()[0][0]

# Créer une vue temporaire pour le DataFrame
flights_df_parquet.createOrReplaceTempView("flights")
airlines_df_parquet.createOrReplaceTempView("airlines")
airports_df_parquet.createOrReplaceTempView("airports")
zones_df_parquet.createOrReplaceTempView("zones")

# Q1. La compagnie avec le + de vols en cours
q1_query = questions_sql_query("Question 1")
try:
    q1_result = spark.sql(q1_query)
    q1_result_path = "../proceded_data/results/q1_result"
    add_max_datetime_and_save_to_parquet(q1_result, q1_result_path, max_datetime)
    print("Q1 terminée")
except Exception as e:
    print("Erreur lors de l'exécution de la question 1 :", str(e))

# Q2. Pour chaque continent, la compagnie avec le + de vols régionaux actifs (continent d'origine == continent de destination)
q2_query = questions_sql_query("Question 2")
try:
    get_continent_udf = F.udf(get_continent, StringType())
    airports_df_parquet_with_continent = airports_df_parquet.withColumn("continent", get_continent_udf(airports_df_parquet["country"]))
    airports_df_parquet_with_continent.createOrReplaceTempView("airports_with_continent")

    q2_table_result = spark.sql(q2_query)
    q2_result_path = "../proceded_data/results/q2_result"
    add_max_datetime_and_save_to_parquet(q2_table_result, q2_result_path, max_datetime)
    print("Q2 terminée")
except Exception as e:
    print("Erreur lors de l'exécution de la question 2 :", str(e))


# Q3. Le vol en cours avec le trajet le plus long
q3_query = questions_sql_query("Question 3")
columns_to_keep = ['id' , 'airline_icao' , 'registration', 'origin_country' , 'destination_country' ,  'origin_airport_icao' , 'destination_airport_icao' , 
                   'origin_airport_name', 'destination_airport_name' , 'distance' ]

try:
    q3_table_result = spark.sql(q3_query)
    q3_pandas_df = q3_table_result.toPandas()

    # Appliquer les fonctions à chaque ligne du DataFrame
    q3_pandas_df['origin_airport'] = q3_pandas_df.apply(lambda row: create_airport_info(row, 'origin'), axis=1)
    q3_pandas_df['destination_airport'] = q3_pandas_df.apply(lambda row: create_airport_info(row, 'destination'), axis=1)
    q3_pandas_df['distance'] = q3_pandas_df.apply(calculate_distance, axis=1)
    q3_pandas_df = q3_pandas_df[q3_pandas_df['distance'] == q3_pandas_df['distance'].max()][columns_to_keep]

    q3_results = spark.createDataFrame(q3_pandas_df)
    q3_result_path = "../proceded_data/results/q3_result"
    add_max_datetime_and_save_to_parquet(q3_results, q3_result_path, max_datetime)
    print("Q3 terminée")
except Exception as e:
    print("Erreur lors de l'exécution de la question 3 :", str(e))


# Q4. Pour chaque continent, la longueur de vol moyenne
q4_query = questions_sql_query("Question 4")

try:
    q4_table_result = spark.sql(q4_query)
    q4_pandas_df = q4_table_result.toPandas()

    # Appliquer les fonctions à chaque ligne du DataFrame
    q4_pandas_df['origin_airport'] = q4_pandas_df.apply(lambda row: create_airport_info(row, 'origin'), axis=1)
    q4_pandas_df['destination_airport'] = q4_pandas_df.apply(lambda row: create_airport_info(row, 'destination'), axis=1)
    q4_pandas_df['distance'] = q4_pandas_df.apply(calculate_distance, axis=1)

    continent_avg_distance = q4_pandas_df.groupby('origin_airport_continent')['distance'].mean().reset_index()
    continent_avg_distance['distance'] = continent_avg_distance['distance'].round(2)
    continent_avg_distance = continent_avg_distance.rename(columns={'origin_airport_continent': 'continent', 'distance': 'average_distance'})

    q4_results = spark.createDataFrame(continent_avg_distance)
    q4_result_path = "../proceded_data/results/q4_result"
    add_max_datetime_and_save_to_parquet(q4_results, q4_result_path, max_datetime)
    print("Q4 terminée")
except Exception as e:
    print("Erreur lors de l'exécution de la question 4 :", str(e))

# Q5. L'entreprise constructeur d'avions avec le plus de vols actifs
q5_query = questions_sql_query("Question 5")
aircrafts_df = spark.read \
    .option("header", "true") \
    .option("delimiter", ";") \
    .csv("../proceded_data/aircrafts_wikipedia/aircrafts.csv")
aircrafts_df.createOrReplaceTempView("aircrafts_wikipedia")

try:
    q5_table_result = spark.sql(q5_query)
    q5_result_path = "../proceded_data/results/q5_result"
    add_max_datetime_and_save_to_parquet(q5_table_result, q5_result_path, max_datetime)
    print("Q5 terminée")
except Exception as e:
    print("Erreur lors de l'exécution de la question 5 :", str(e))


# Q6. Pour chaque pays de compagnie aérienne, le top 3 des modèles d'avion en usage
q6_query = questions_sql_query("Question 6")
airlines_by_country_wikipedia_path = "../proceded_data/airlines_by_country_wikipedia/airlines_by_country.csv"
airlines_by_country_wikipedia_df = spark.read \
    .option("header", "true") \
    .option("delimiter", ";") \
    .csv(airlines_by_country_wikipedia_path)
airlines_by_country_wikipedia_df.createOrReplaceTempView("airlines_by_country_wikipedia")

try:
    q6_table_result = spark.sql(q6_query)
    q6_result_path = "../proceded_data/results/q6_result"
    add_max_datetime_and_save_to_parquet(q6_table_result, q6_result_path, max_datetime)
    print("Q6 terminée")
except Exception as e:
    print("Erreur lors de l'exécution de la question 6 :", str(e))

# Bonus. Quel aéroport a la plus grande différence entre le nombre de vol sortant et le nombre de vols entrants ?
bonus_query = questions_sql_query("Question bonus")

try:
    bonus_table_result = spark.sql(bonus_query)
    bonus_result_path = '../proceded_data/results/bonus_result'
    add_max_datetime_and_save_to_parquet(bonus_table_result, bonus_result_path, max_datetime)
    print("Bonus terminé")
except Exception as e:
    print("Erreur lors de l'exécution de la question bonus :", str(e))


# Terminer la session de Spark
spark.stop()
