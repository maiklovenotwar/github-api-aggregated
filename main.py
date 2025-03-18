import requests
import time
import datetime
from datetime import timedelta
from src.github_database.api.github_api import search_repositories
from src.github_database.api.github_contributors import get_contributors_for_repo
from src.github_database.database.database import init_db, get_session, create_repository_from_api, create_contributor_from_api
from src.github_database.database.database import Contributor, Repository
from dotenv import load_dotenv
import os

# Lade Umgebungsvariablen
load_dotenv()
GITHUB_TOKEN = os.getenv('GITHUB_API_TOKEN')
headers = {
    'Authorization': f'token {GITHUB_TOKEN}',
    'Accept': 'application/vnd.github.v3+json'
}

def check_rate_limit(threshold=10):
    """
    Überprüft das aktuelle API-Rate-Limit.
    Wenn die verbleibenden Aufrufe unter dem Schwellenwert liegen,
    wird bis zum Reset-Zeitpunkt (plus einem kleinen Puffer) gewartet.
    """
    url = "https://api.github.com/rate_limit"
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        core = data.get("resources", {}).get("core", {})
        remaining = core.get("remaining", 0)
        reset = core.get("reset", 0)
        reset_time = datetime.datetime.fromtimestamp(reset)
        print(f"Rate-Limit verbleibend: {remaining} (Reset: {reset_time})")
        if remaining < threshold:
            sleep_seconds = (reset_time - datetime.datetime.now()).total_seconds() + 5  # 5 Sekunden Puffer
            if sleep_seconds > 0:
                print(f"Rate-Limit niedrig. Warte {int(sleep_seconds)} Sekunden...")
                time.sleep(sleep_seconds)
    else:
        print(f"Fehler beim Abrufen des Rate-Limits: HTTP {response.status_code}")

def main():
    # Initialisiere die Datenbank (erstellt alle Tabellen, falls noch nicht vorhanden)
    init_db()
    
    # Öffne eine Datenbanksession
    session = get_session()
    
    # Überprüfe zu Beginn das Rate-Limit
    check_rate_limit()
    
    # Prüfe, ob bereits Repositories in der Datenbank vorhanden sind
    last_repo = session.query(Repository).order_by(Repository.created_at.desc()).first()
    if last_repo and last_repo.created_at:
        # Ziehe einen Tag zurück, um sicherzustellen, dass auch Repositories erfasst werden,
        # die denselben Zeitstempel wie der letzte Eintrag haben.
        start_date = last_repo.created_at - timedelta(days=1)
        query = f"created:>{start_date.strftime('%Y-%m-%dT%H:%M:%SZ')}"
        print(f"Abfrage neuer Repositories, erstellt nach: {start_date}")
    else:
        # Falls noch keine Repositories vorhanden sind, verwende einen Standardzeitraum (hier z. B. 2022)
        query = "created:2022-01-01..2022-12-31"
        print("Keine vorhandenen Repositories gefunden. Abfrage für Repositories aus 2022.")
    
    # Abruf der Repositories über die Search API
    repositories = search_repositories(query, per_page=100, max_pages=5)
    print("Insgesamt gefundene Repositories:", len(repositories))
    
    for repo_data in repositories:
        # Überprüfe das Rate-Limit vor jedem Repository
        check_rate_limit()
        try:
            if isinstance(repo_data, dict):
                repo_obj = create_repository_from_api(repo_data)
            else:
                repo_obj = repo_data
                
            # Update-Logik: Falls das Repository bereits existiert, könnten hier auch
            # Felder aktualisiert werden, die sich ändern (z.B. stars_count, forks_count, etc.)
            existing_repo = session.query(Repository).filter_by(repo_id=repo_obj.repo_id).first()
            if not existing_repo:
                session.add(repo_obj)
            else:
                print(f"Repository {repo_obj.name} existiert bereits. Überspringe Einfügen.")
                # Hier könnte eine Update-Logik implementiert werden, falls sich Felder ändern sollen.
            
            # Rufe Contributor-Daten für das Repository ab (nur, wenn API-Daten als Dict vorliegen)
            if isinstance(repo_data, dict):
                contributors_data = get_contributors_for_repo(repo_data)
                print(f"{len(contributors_data)} Contributor gefunden für Repository {repo_obj.name}")
                
                for contrib in contributors_data:
                    check_rate_limit()  # Prüfe auch in der Contributor-Schleife das Rate-Limit
                    existing_contrib = session.query(Contributor).filter_by(user_id=contrib.get("id")).first()
                    if not existing_contrib:
                        contributor_obj = create_contributor_from_api(contrib)
                        session.add(contributor_obj)
                    else:
                        # Aktualisiere ggf. dynamisch änderbare Felder
                        existing_contrib.contributions = contrib.get("contributions")
                        
        except Exception as e:
            session.rollback()
            print("Fehler beim Verarbeiten eines Repositories:", e)
    
    session.commit()
    print("Repository- und Contributor-Daten wurden in der Datenbank gespeichert.")

if __name__ == "__main__":
    main()