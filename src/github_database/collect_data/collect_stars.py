import requests
import time
import datetime
from dotenv import load_dotenv
from src.github_database.database.database import get_session, Repository, Star, Contributor, create_contributor_from_api
import os

# Lade Umgebungsvariablen und setze den API-Header.
load_dotenv()
GITHUB_TOKEN = os.getenv('GITHUB_API_TOKEN')
# Für das Abrufen von Star-Daten inklusive "starred_at" verwenden wir den entsprechenden Accept-Header:
headers = {
    'Authorization': f'token {GITHUB_TOKEN}',
    'Accept': 'application/vnd.github.v3.star+json'
}

def get_repo_details(repo_id):
    """
    Ruft vollständige Repository-Details von GitHub ab.
    """
    url = f"https://api.github.com/repositories/{repo_id}"
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Fehler beim Abrufen von Repository-Details für {repo_id}: HTTP {response.status_code}")
        return None

def search_stars(owner_login, repo_name, per_page=100, max_pages=5):
    """
    Ruft die Stars eines Repositories ab.
    Nutzt den Endpunkt: GET /repos/{owner}/{repo}/stargazers.
    Liefert eine Liste von Dictionaries zurück, die jeweils den Star-Eintrag (mit "starred_at" und "user") enthalten.
    """
    stars = []
    for page in range(1, max_pages + 1):
        url = f"https://api.github.com/repos/{owner_login}/{repo_name}/stargazers?per_page={per_page}&page={page}"
        print(f"Abfrage von Stars: Seite {page} für {owner_login}/{repo_name}")
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            if not data:
                print("Keine weiteren Stars gefunden.")
                break
            stars.extend(data)
            time.sleep(1)  # Rate-Limit schonen
        else:
            print(f"Fehler beim Abrufen der Stars auf Seite {page} für {owner_login}/{repo_name}: HTTP {response.status_code}")
            break
    return stars

def create_star_from_api(api_data, repository_internal_id):
    """
    Wandelt API-Daten eines Star-Eintrags in ein Star-Objekt um.
    
    Erwartete Felder:
      - "starred_at": Zeitpunkt, an dem der Star vergeben wurde (als String, der in datetime umgewandelt wird)
      - "user": Das User-Objekt des Star-Gebers, aus dem die GitHub-ID extrahiert wird.
    
    Das erstellte Star-Objekt wird mit dem internen Repository-Eintrag verknüpft.
    """
    starred_at_str = api_data.get("starred_at")
    starred_at = None
    if starred_at_str:
        try:
            starred_at = datetime.datetime.strptime(starred_at_str, '%Y-%m-%dT%H:%M:%SZ')
        except Exception as e:
            print(f"Fehler beim Parsen von starred_at: {e}")
    # Extrahiere die User-Daten
    user_data = api_data.get("user")
    user_id = user_data.get("id") if user_data else None
    
    star_obj = Star(
        repo_id=repository_internal_id,
        starred_by=user_id,
        starred_at=starred_at
    )
    return star_obj

def update_stars():
    session = get_session()
    
    # Lade alle Repositories aus der Datenbank
    repositories = session.query(Repository).all()
    print(f"{len(repositories)} Repositories in der Datenbank gefunden.")
    
    for repo_obj in repositories:
        try:
            # Hole die Repository-Details, um Owner-Login und Repository-Namen zu erhalten
            repo_details = get_repo_details(repo_obj.repo_id)
            if not repo_details:
                continue
            owner_login = repo_details.get("owner", {}).get("login")
            repo_name = repo_details.get("name")
            if not owner_login or not repo_name:
                print(f"Unvollständige Daten für Repository {repo_obj.repo_id}. Überspringe.")
                continue
            
            # Abruf der Star-Daten über die API
            stars_data = search_stars(owner_login, repo_name, per_page=100, max_pages=5)
            print(f"{len(stars_data)} Stars gefunden für Repository {repo_obj.name}")
            
            for star_data in stars_data:
                # Extrahiere die GitHub-ID des Star-Gebers aus dem "user"-Objekt
                star_user_id = star_data.get("user", {}).get("id")
                if not star_user_id:
                    print("Kein User-ID für den Star gefunden. Überspringe diesen Eintrag.")
                    continue
                
                # Stelle sicher, dass der entsprechende Contributor in der DB existiert
                local_contrib = session.query(Contributor).filter_by(user_id=star_user_id).first()
                if not local_contrib:
                    minimal_contrib = {
                        "id": star_user_id,
                        "login": star_data.get("user", {}).get("login"),
                        "contributions": 0
                    }
                    local_contrib = create_contributor_from_api(minimal_contrib)
                    session.add(local_contrib)
                    session.commit()  # Damit wird die interne ID zugewiesen
                
                # Prüfe, ob dieser Star bereits in der DB existiert (basierend auf Kombination von repo_id und starred_by)
                existing_star = session.query(Star).filter_by(repo_id=repo_obj.id, starred_by=star_user_id).first()
                starred_at_str = star_data.get("starred_at")
                new_starred_at = None
                if starred_at_str:
                    try:
                        new_starred_at = datetime.datetime.strptime(starred_at_str, '%Y-%m-%dT%H:%M:%SZ')
                    except Exception as e:
                        print(f"Fehler beim Parsen von starred_at: {e}")
                
                if not existing_star:
                    star_obj = create_star_from_api(star_data, repo_obj.id)
                    session.add(star_obj)
                    print(f"Star von User {local_contrib.username} für Repository {repo_obj.name} hinzugefügt.")
                else:
                    # Update-Logik: Falls das Datum (starred_at) sich geändert hat, aktualisieren
                    if new_starred_at and new_starred_at != existing_star.starred_at:
                        existing_star.starred_at = new_starred_at
                        print(f"Star von User {local_contrib.username} für Repository {repo_obj.name} aktualisiert.")
                    else:
                        print(f"Star von User {local_contrib.username} für Repository {repo_obj.name} ist aktuell. Überspringe Update.")
            time.sleep(1)
        except Exception as e:
            session.rollback()
            print(f"Fehler beim Verarbeiten der Stars für Repository {repo_obj.name}: {e}")
    
    session.commit()
    print("Star-Daten wurden in der Datenbank aktualisiert.")

if __name__ == "__main__":
    update_stars()