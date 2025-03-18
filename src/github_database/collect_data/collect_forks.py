import requests
import time
import datetime
from dotenv import load_dotenv
from src.github_database.database.database import get_session, Repository, Fork, Contributor
import os

# Lade Umgebungsvariablen und setze API-Header
load_dotenv()
GITHUB_TOKEN = os.getenv('GITHUB_API_TOKEN')
headers = {
    'Authorization': f'token {GITHUB_TOKEN}',
    'Accept': 'application/vnd.github.v3+json'
}

def get_repo_details(repo_id):
    """
    Ruft die vollständigen Details eines Repositories anhand seiner GitHub-spezifischen ID ab.
    Diese Daten enthalten u.a. den Owner-Login und den Repository-Namen, die für den API-Aufruf der Forks benötigt werden.
    """
    url = f"https://api.github.com/repositories/{repo_id}"
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Fehler beim Abrufen der Repository-Details für {repo_id}: HTTP {response.status_code}")
        return None

def search_forks(owner_login, repo_name, per_page=100, max_pages=5):
    """
    Ruft die Forks eines Repositories ab.
    
    Parameter:
      - owner_login: GitHub-Login des Repository-Besitzers
      - repo_name: Name des Repositories
      - per_page: Ergebnisse pro Seite (maximal 100)
      - max_pages: Maximale Anzahl der abzurufenden Seiten
      
    Gibt eine Liste von Fork-Dictionaries zurück.
    """
    forks = []
    for page in range(1, max_pages + 1):
        url = f"https://api.github.com/repos/{owner_login}/{repo_name}/forks?per_page={per_page}&page={page}"
        print(f"Abfrage von Forks: Seite {page} für {owner_login}/{repo_name}")
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            if not data:
                print("Keine weiteren Forks gefunden.")
                break
            forks.extend(data)
            time.sleep(1)  # Kurze Pause zur Schonung der Rate-Limits
        else:
            print(f"Fehler beim Abrufen der Forks auf Seite {page} für {owner_login}/{repo_name}: HTTP {response.status_code}")
            break
    return forks

def create_fork_from_api(api_data, repository_internal_id, local_contributor_id):
    """
    Wandelt API-Daten eines Forks in ein Fork-Objekt um.
    
    Felder:
      - repo_id: Interne ID des Original-Repositories.
      - forked_by: Lokale Contributor-ID des Forkers.
      - created_at: Datum, an dem der Fork erstellt wurde (als datetime).
    """
    created_at_str = api_data.get("created_at")
    created_at = None
    if created_at_str:
        try:
            created_at = datetime.datetime.strptime(created_at_str, '%Y-%m-%dT%H:%M:%SZ')
        except Exception as e:
            print(f"Fehler beim Parsen von created_at für Fork: {e}")
    fork_obj = Fork(
        repo_id=repository_internal_id,
        forked_by=local_contributor_id,
        created_at=created_at
    )
    return fork_obj

def update_forks():
    session = get_session()
    
    # Lade alle Repositories aus der Datenbank
    repositories = session.query(Repository).all()
    print(f"{len(repositories)} Repositories in der Datenbank gefunden.")
    
    for repo_obj in repositories:
        try:
            # Hole vollständige Repository-Daten, um Owner-Login und Repo-Namen zu erhalten
            repo_details = get_repo_details(repo_obj.repo_id)
            if not repo_details:
                continue
            owner_login = repo_details.get("owner", {}).get("login")
            repo_name = repo_details.get("name")
            if not owner_login or not repo_name:
                print(f"Unvollständige Daten für Repository {repo_obj.repo_id}. Überspringe.")
                continue
            
            # Abruf der Forks über die API
            forks_data = search_forks(owner_login, repo_name, per_page=100, max_pages=5)
            print(f"{len(forks_data)} Forks gefunden für Repository {repo_obj.name}")
            
            for fork_data in forks_data:
                # Extrahiere die GitHub-ID des Forkers
                forker_user_id = fork_data.get("owner", {}).get("id")
                if not forker_user_id:
                    print("Kein Fork-User-ID gefunden. Überspringe diesen Fork.")
                    continue
                
                # Überprüfe, ob der Fork-Owner bereits als Contributor in der DB existiert
                local_contrib = session.query(Contributor).filter_by(user_id=forker_user_id).first()
                if not local_contrib:
                    # Erstelle einen minimalen Contributor, falls nicht vorhanden
                    minimal_contrib = {
                        "id": forker_user_id,
                        "login": fork_data.get("owner", {}).get("login"),
                        "contributions": 0
                    }
                    from src.github_database.database.database import create_contributor_from_api
                    local_contrib = create_contributor_from_api(minimal_contrib)
                    session.add(local_contrib)
                    session.commit()  # Commit, damit local_contrib.id zugewiesen wird
                
                # Prüfe, ob dieser Fork bereits in der DB existiert (basierend auf repo_id und forked_by)
                existing_fork = session.query(Fork).filter_by(repo_id=repo_obj.id, forked_by=local_contrib.id).first()
                if not existing_fork:
                    # Falls nicht vorhanden, füge neuen Fork hinzu
                    fork_obj = create_fork_from_api(fork_data, repo_obj.id, local_contrib.id)
                    session.add(fork_obj)
                    print(f"Neuer Fork von {local_contrib.username} für Repository {repo_obj.name} hinzugefügt.")
                else:
                    # Update-Logik: Falls der Fork bereits existiert, prüfen wir, ob sich das Erstellungsdatum geändert hat.
                    new_created_at = None
                    created_at_str = fork_data.get("created_at")
                    if created_at_str:
                        try:
                            new_created_at = datetime.datetime.strptime(created_at_str, '%Y-%m-%dT%H:%M:%SZ')
                        except Exception as e:
                            print(f"Fehler beim Parsen von created_at für Fork: {e}")
                    if new_created_at and existing_fork.created_at != new_created_at:
                        existing_fork.created_at = new_created_at
                        print(f"Fork von {local_contrib.username} für Repository {repo_obj.name} aktualisiert.")
                    else:
                        print(f"Fork von {local_contrib.username} für Repository {repo_obj.name} ist aktuell. Überspringe.")
            time.sleep(1)
        except Exception as e:
            session.rollback()
            print(f"Fehler beim Verarbeiten der Forks für Repository {repo_obj.name}: {e}")
    
    session.commit()
    print("Fork-Daten wurden in der Datenbank aktualisiert.")

if __name__ == "__main__":
    update_forks()