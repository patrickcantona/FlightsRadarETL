import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Fonction pour créer une session Spark
def create_spark_session(app_name: str):
    try:
        # Créer et retourner une session Spark avec le nom spécifié
        spark = SparkSession.builder.appName(app_name).getOrCreate()
        return spark
    except Exception as e:
        # Gérer les exceptions lors de la création de la session Spark
        print(f"Erreur lors de la création de la session Spark : {e}")
        return None

# Fonction pour créer un DataFrame Spark à partir d'une liste de données
def create_dataframe_from_list(data: list, spark):
    try:
        # Essayer de créer un DataFrame Spark à partir de la liste de données
        df = spark.createDataFrame(data)
    except Exception as e:
        # Gérer les exceptions lors de la création du DataFrame
        print("Erreur lors de la création du DataFrame directement à partir de la liste:", e)
        print("Chargement des données dans un RDD et conversion en DataFrame...")
        try:
            # Charger les données dans un RDD Spark
            rdd = spark.sparkContext.parallelize(data)
            # Convertir le RDD en DataFrame
            df = rdd.toDF()
        except Exception as e:
            # Gérer les exceptions lors du chargement des données dans un RDD et de la conversion en DataFrame
            print("Erreur lors du chargement des données dans un RDD et de la conversion en DataFrame:", e)
            df = None
    
    return df

# Fonction pour créer un DataFrame Spark à partir d'un fichier Parquet avec un schéma spécifié
def create_dataframe_from_parquet_with_schema(parquet_path: str, spark, schema):
    try:
        # Charger le DataFrame à partir du fichier Parquet
        df = spark.read.parquet(parquet_path)
        
        # Appliquer le schéma spécifié au DataFrame
        for field in schema.fields:
            if field.name not in df.columns:
                raise ValueError(f"Le champ {field.name} n'existe pas dans les données réelles.")
            if field.dataType != df.schema[field.name].dataType:
                raise ValueError(f"Le champ {field.name} a un type de données différent.")
        
        return df
    except Exception as e:
        # Gérer les exceptions lors du chargement du DataFrame à partir du fichier Parquet avec un schéma spécifié
        print("Une erreur s'est produite lors du chargement du DataFrame :", str(e))
        return None

# Fonction pour créer un DataFrame Spark à partir d'un fichier Parquet
def create_dataframe_from_parquet(spark, filepath):
    # Charger le fichier Parquet en tant que DataFrame Spark
    df = spark.read.parquet(filepath)
    return df

# Fonction pour créer un DataFrame Spark à partir d'un fichier JSON
def create_dataframe_from_json(spark, filepath):
    # Charger le fichier JSON en tant que DataFrame Spark
    df = spark.read.json(filepath)
    return df
