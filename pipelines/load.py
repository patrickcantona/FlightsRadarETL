import os
from pyspark.sql.functions import year, month, dayofmonth
import json
import pyspark.sql.functions as F

# Fonction pour sauvegarder un DataFrame au format Parquet en utilisant la date actuelle comme répertoire de destination
def save_dataframe_as_parquet_with_current_date(df, base_path :str , current_date , mode :str):
    # Construire le chemin du fichier en fonction de la date actuelle
    file_path = os.path.join(base_path, f'year={current_date.year}', f'month={current_date.month}', f'day={current_date.day}')
    
    # Écrire le DataFrame au format Parquet avec le mode spécifié
    df.write.mode(mode).parquet(file_path)

# Fonction pour sauvegarder un dictionnaire au format JSON en utilisant la date actuelle comme répertoire de destination
def save_dict_as_json_with_current_date(data_dict, base_path :str , current_date ):
    # Construire le chemin du fichier JSON en fonction de la date actuelle
    file_path = os.path.join(base_path, f'year={current_date.year}', f'month={current_date.month}', f'day={current_date.day}', 'data.json')
    
    # Créer les répertoires nécessaires pour le fichier JSON
    os.makedirs(os.path.dirname(file_path), exist_ok=True)  
    
    # Écrire le dictionnaire au format JSON dans le fichier
    with open(file_path, 'w') as f:
        json.dump(data_dict, f)

# Fonction pour sauvegarder un DataFrame au format Parquet en partitionnant par colonne datetime
def save_dataframe_as_parquet_with_column_datetime(df, path : str , datetime_column , mode : str ):
    # Ajouter les colonnes year, month et day au DataFrame en utilisant les fonctions year(), month() et dayofmonth()
    df.withColumn("year", year(datetime_column)) \
      .withColumn("month", month(datetime_column)) \
      .withColumn("day", dayofmonth(datetime_column)) \
      .write.partitionBy("year", "month", "day").mode(mode).parquet(path)

# Fonction pour ajouter une colonne datetime avec une valeur spécifique et sauvegarder le DataFrame au format Parquet. Cette fonction est utiliser pour sauvegerder les resultats
def add_max_datetime_and_save_to_parquet(df , basepath, max_datetime):

    # Ajouter une colonne datetime avec la valeur max_datetime
    df_with_datetime = df.withColumn('datetime', F.lit(max_datetime))
    
    # Extraire les informations sur l'année, le mois et le jour de max_datetime
    year, month, day = str(max_datetime.year), str(max_datetime.month).zfill(2), str(max_datetime.day).zfill(2)
    
    # Créer le chemin complet pour enregistrer le fichier Parquet
    output_path = os.path.join(basepath, f'year={year}', f'month={month}', f'day={day}')

    # Vérifier si le dossier de sortie existe, sinon le créer
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    
    # Enregistrer le DataFrame au format Parquet avec le mode 'append'
    df_with_datetime.write.mode('append').parquet(output_path)
