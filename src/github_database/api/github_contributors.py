import requests
import time
from dotenv import load_dotenv
import os
from datetime import datetime

load_dotenv()
GITHUB_TOKEN = os.getenv('GITHUB_API_TOKEN')

headers = {
    'Authorization': f'token {GITHUB_TOKEN}',
    'Accept': 'application/vnd.github.v3+json'
}

def get_contributors_for_repo(repo_data):
    """
    Ruft die Contributor-Daten für ein Repository ab.
    Erwartet, dass repo_data den Schlüssel "contributors_url" enthält.
    Gibt eine Liste von Contributor-Dictionaries zurück.
    
    Mit integrierter Rate-Limit-Prüfung und Fehlerbehandlung.
    """
    url = repo_data.get("contributors_url")
    if not url:
        print("Kein 'contributors_url' im Repository-Datensatz gefunden.")
        return []
    
    contributors = []
    page = 1
    per_page = 100

    while True:
        full_url = f"{url}?per_page={per_page}&page={page}"
        print(f"Abfrage von Contributors: Seite {page}")
        response = requests.get(full_url, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            if not data:
                break
            contributors.extend(data)
            # Wenn weniger als per_page Ergebnisse zurückkommen, ist das die letzte Seite.
            if len(data) < per_page:
                break
            page += 1
            # Warte, um die Rate-Limits zu schonen
            time.sleep(1)
        elif response.status_code == 403:
            # Bei Rate-Limit-Fehler: Warte bis zum Reset (eventuell erweitern)
            reset_time = response.headers.get("X-RateLimit-Reset")
            if reset_time:
                wait_seconds = int(reset_time) - int(time.time()) + 5
                print(f"Rate-Limit erreicht. Warte {wait_seconds} Sekunden...")
                time.sleep(wait_seconds)
                continue
            else:
                print("Rate-Limit-Fehler, aber kein Reset-Zeitstempel vorhanden.")
                break
        else:
            print(f"Fehler beim Abrufen der Contributors: HTTP {response.status_code}")
            break

    return contributors