import schedule
import time
import os

def run_script():
    os.system("python flights_pipeline.py")

# Planifiez l'exécution de votre script toutes les 2 heures
schedule.every(1).hours.do(run_script)



# Boucle d'exécution pour vérifier périodiquement s'il est temps d'exécuter le script
while True:
    schedule.run_pending()
    time.sleep(60)