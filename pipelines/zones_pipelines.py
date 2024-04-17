import os
from extract import *
from clean_transform import *
from load import *
from FlightRadar24 import FlightRadar24API
from datetime import datetime

def main():
    print("Exécution du pipeline de zones_pipeline.py responsable de l'ETL des données des zones")
    # Initialisation de l'API FlightRadar24
    fr_api = FlightRadar24API()

    # Chemins des fichiers JSON pour les données brutes et nettoyées des zones
    zones_raw_json_files_path = "../proceded_data/zones/raw/"
    zones_clean_json_file_path = "../proceded_data/zones/clean/zones.json"

    # Date actuelle
    current_date = datetime.now()

    # Récupération des données des zones
    zones = fr_api.get_zones()

    # Sauvegarde des données brutes au format JSON avec la date actuelle dans le nom de fichier
    save_dict_as_json_with_current_date(zones, zones_raw_json_files_path , current_date )

    # Traitement des données des zones et sauvegarde du résultat dans un fichier JSON nettoyé
    process_zones_data(zones, zones_clean_json_file_path)

    # Affichage du message de fin du traitement
    print("Pipeline des zones terminé")

if __name__ == "__main__":
    main()
