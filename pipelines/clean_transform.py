import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import pandas as pd
from FlightRadar24 import Airport  # Importation de la classe Airport pour manipuler les aéroports
import pycountry_convert as pc  # Module pour convertir les noms de pays en codes de continent

# Fonction pour supprimer les doublons dans un DataFrame
def remove_duplicates(df):
    columns_to_drop = ['year', 'month', 'day']
    columns_to_drop = [col for col in columns_to_drop if col in df.columns]  # Filtrer les colonnes existantes dans le DataFrame
    if columns_to_drop:
        df = df.drop(*columns_to_drop).dropDuplicates()  # Supprimer les doublons basés sur les colonnes spécifiées
    else:
        df = df.dropDuplicates()  # Supprimer les doublons sans spécifier de colonnes
    return df


# Fonction pour supprimer les doublons basés sur une colonne spécifique
def remove_duplicates_by_column(df, column):
    if "time" in df.columns:
        df = df.orderBy(F.col("time").desc())  # Ordonner le DataFrame par la colonne 'time' de manière décroissante
    df = df.dropDuplicates([column])  # Supprimer les doublons basés sur la colonne spécifiée
    return df
    

# Fonction pour supprimer les espaces inutiles dans les colonnes de type chaîne de caractères
def trim_string_columns(df):
    string_columns = [column[0] for column in df.dtypes  if column[1].startswith('string')]  # Récupérer les colonnes de type chaîne de caractères
    for column in string_columns:
        df = df.withColumn(column, F.trim(column))  # Supprimer les espaces inutiles dans les valeurs des colonnes de type chaîne de caractères
    return df

# Fonction pour arrondir les colonnes de type double à deux décimales
def round_double_columns(df):
    double_columns = [column[0] for column in df.dtypes  if column[1].startswith('double')]  # Récupérer les colonnes de type double
    for column in double_columns:
        df = df.withColumn(column, F.round(column, 2))  # Arrondir les valeurs des colonnes de type double à deux décimales
    return df

# Fonction pour filtrer les lignes ayant une colonne spécifique non vide
def filter_not_empty_column(df, not_empty_column : str):
    df = df.filter(F.col(not_empty_column) != "")  # Filtrer les lignes ayant la colonne spécifiée non vide
    return df

# Fonction pour ajouter une colonne de date et d'heure à partir d'une colonne de temps
def add_datetime_column(df, time_column):
    if time_column is None:
        return df
    else:
        return df.withColumn("datetime", F.to_timestamp(F.col(time_column)))  # Convertir la colonne spécifiée en colonne de date et d'heure

# Fonction pour exécuter une série de transformations sur un DataFrame Spark
def run_spark_df_pipeline(df, column , not_empty_column, time_column):
    cleaned_df = df \
        .transform(remove_duplicates) \
        .transform(remove_duplicates_by_column , column) \
        .transform(trim_string_columns) \
        .transform(round_double_columns) \
        .transform(filter_not_empty_column, not_empty_column) \
        .transform(add_datetime_column, time_column)
    return cleaned_df

# Fonction pour extraire les coordonnées des zones à partir d'un dictionnaire
def extract_zones_coordinates(zone, subzone=None):
    if subzone:
        subzone_data = zone['subzones'][subzone]
        return subzone, subzone_data['tl_y'], subzone_data['tl_x'], subzone_data['br_y'], subzone_data['br_x']
    else:
        return None, zone['tl_y'], zone['tl_x'], zone['br_y'], zone['br_x']

# Fonction pour créer un DataFrame à partir d'un dictionnaire de zones
def create_dataframe_from_zones_dict(data_dict):
    data = []
    for continent, continent_data in data_dict.items():
        continent_row = extract_zones_coordinates(continent_data)
        data.append((continent, *continent_row))
        
        if 'subzones' in continent_data:
            for subzone, subzone_data in continent_data['subzones'].items():
                subzone_row = extract_zones_coordinates(continent_data, subzone)
                data.append((continent, *subzone_row))
                
                if 'subzones' in subzone_data:
                    for sub_subzone, sub_subzone_data in subzone_data['subzones'].items():
                        sub_subzone_row = extract_zones_coordinates(subzone_data, sub_subzone)
                        data.append((continent, *sub_subzone_row))

    columns = ['continent', 'subzone', 'tl_y', 'tl_x', 'br_y', 'br_x']
    df = pd.DataFrame(data, columns=columns)
    
    return df

# Fonction pour nettoyer le DataFrame des zones
def clean_zones_dataframe(df):
    # Supprimer les lignes où la colonne 'continent' est vide
    df = df.dropna(subset=['continent'])

    # Supprimer les lignes où les coordonnées 'tl' et 'br' sont vides
    df = df.dropna(subset=['tl_y', 'tl_x', 'br_y', 'br_x'])

    # Supprimer les doublons
    df = df.drop_duplicates()
    
    # Convertir les colonnes de coordonnées en float et limiter la précision à 2 chiffres après la virgule
    df[['tl_y', 'tl_x', 'br_y', 'br_x']] = df[['tl_y', 'tl_x', 'br_y', 'br_x']].astype(float).round(2)
    
    # Trim les valeurs dans les colonnes 'continent' et 'subzone'
    df['continent'] = df['continent'].str.strip()
    df['subzone'] = df['subzone'].str.strip()
    
    return df

# Fonction pour sauvegarder un DataFrame au format JSON
def save_dataframe_to_json(df, filepath):
    df.to_json(filepath, orient='records')

# Fonction pour traiter les données des zones et sauvegarder le résultat au format JSON
def process_zones_data(data_dict, output_filepath):
    # Extraction des données de zones pour créer un DataFrame
    zones_df = create_dataframe_from_zones_dict(data_dict)
    
    # Nettoyage du DataFrame
    cleaned_zones_df = clean_zones_dataframe(zones_df)
    
    # Sauvegarde du DataFrame au format JSON
    save_dataframe_to_json(cleaned_zones_df, output_filepath)

# Fonction pour créer un objet Airport à partir d'une ligne de DataFrame et d'une localisation spécifique
def create_airport_info(row, location):
    info = {
        "lat": float(row[f'{location}_latitude']),
        "lon": float(row[f'{location}_longitude']),
        "alt": float(row[f'{location}_altitude']),
        "name": row[f'{location}_airport_name'],
        "icao": row.get(f'{location}_airport_icao', ''),  
        "iata": row[f'{location}_airport_iata'],
        "country": row[f'{location}_country']
    }
    return Airport(basic_info=info)

# Fonction pour calculer la distance entre deux aéroports à partir d'une ligne de DataFrame
def calculate_distance(row):
    origin_airport = create_airport_info(row, 'origin')
    destination_airport = create_airport_info(row, 'destination')
    
    try:
        distance = round(origin_airport.get_distance_from(destination_airport), 2)  # Calculer la distance entre les deux aéroports et arrondir le résultat à deux décimales
    except Exception as e:
        print(f"An error occurred while calculating distance: {e}")
        distance = None
    
    return distance


# Fonction pour obtenir le minimum et le maximum de la colonne datetime dans un DataFrame Spark
def min_max_datetime(spark, df):
    # Obtenir le minimum et le maximum de la colonne datetime
    min_max_df = df.select(F.min("time").alias("min"), F.max("time").alias("max"))
    
    return min_max_df

# Fonction pour obtenir le continent à partir du nom de pays en utilisant la librairie pycountry_convert
def get_continent(country):
    try:
        country_code = pc.country_name_to_country_alpha2(country)  # Obtenir le code de pays à partir du nom de pays
        continent_code = pc.country_alpha2_to_continent_code(country_code)  # Obtenir le code de continent à partir du code de pays
        continent_name = pc.convert_continent_code_to_continent_name(continent_code)  # Obtenir le nom de continent à partir du code de continent
        return continent_name
    except:
        return None  # Retourner None si une erreur se produit lors de la conversion

