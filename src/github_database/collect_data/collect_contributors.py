import requests
import os
import time
from datetime import datetime
from dotenv import load_dotenv
from github_database.api.github_contributors import get_contributors_for_repo
from github_database.database.database import get_session, create_contributor_from_api, Repository, Contributor

# Lade die Umgebungsvariablen
load_dotenv()
GITHUB_TOKEN = os.getenv('GITHUB_API_TOKEN')
headers = {
    'Authorization': f'token {GITHUB_TOKEN}',
    'Accept': 'application/vnd.github.v3+json'
}

def get_user_details(username):
    """
    Ruft zusätzliche Benutzerdaten vom /users/{username}-Endpunkt ab.
    """
    url = f"https://api.github.com/users/{username}"
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Fehler beim Abrufen von Details für {username}: HTTP {response.status_code}")
        return {}

def update_contributors():
    session = get_session()
    
    # Lade alle Repositories aus der Datenbank, für die wir Contributor-Daten abrufen möchten
    repositories = session.query(Repository).all()
    print(f"{len(repositories)} Repositories in der Datenbank gefunden.")
    
    for repo_obj in repositories:
        try:
            # Erstelle (oder konstruiere) die URL für die Contributor-Daten
            if hasattr(repo_obj, 'contributors_url') and repo_obj.contributors_url:
                api_data = {'contributors_url': repo_obj.contributors_url}
            else:
                # Falls kein contributors_url-Feld vorhanden ist, konstruieren wir die URL basierend auf dem Repository-Namen.
                # Hier solltest du idealerweise auch den Owner berücksichtigen, falls du ihn separat speicherst.
                api_data = {'contributors_url': f"https://api.github.com/repos/{repo_obj.name}/contributors"}
            
            contributors_data = get_contributors_for_repo(api_data)
            print(f"{len(contributors_data)} Contributor gefunden für Repository {repo_obj.name}")
            
            for contrib in contributors_data:
                # Hole zusätzliche User-Details vom /users/{username}-Endpunkt
                user_details = get_user_details(contrib.get("login"))
                
                # Integriere die zusätzlichen Felder in das Contributor-Dictionary
                contrib["email"] = user_details.get("email")
                contrib["company"] = user_details.get("company")
                contrib["location"] = user_details.get("location")
                contrib["created_at"] = user_details.get("created_at")
                
                # Überprüfe, ob der Contributor bereits in der DB existiert
                existing_contrib = session.query(Contributor).filter_by(user_id=contrib.get("id")).first()
                if not existing_contrib:
                    # Erstelle das Contributor-Objekt mit den zusätzlichen Feldern
                    contributor_obj = create_contributor_from_api(contrib)
                    session.add(contributor_obj)
                    print(f"Contributor {contrib.get('login')} hinzugefügt.")
                else:
                    # Update-Logik: Aktualisiere alle Felder, falls sich Werte geändert haben
                    updated = False
                    if existing_contrib.username != contrib.get("login"):
                        existing_contrib.username = contrib.get("login")
                        updated = True
                    if existing_contrib.contributions != contrib.get("contributions"):
                        existing_contrib.contributions = contrib.get("contributions")
                        updated = True
                    if existing_contrib.email != contrib.get("email"):
                        existing_contrib.email = contrib.get("email")
                        updated = True
                    if existing_contrib.company != contrib.get("company"):
                        existing_contrib.company = contrib.get("company")
                        updated = True
                    if existing_contrib.location != contrib.get("location"):
                        existing_contrib.location = contrib.get("location")
                        updated = True
                    # Für created_at: Da dies in der Regel einmalig ist, setzen wir es nur, falls noch nicht vorhanden.
                    if not existing_contrib.created_at and contrib.get("created_at"):
                        try:
                            existing_contrib.created_at = datetime.strptime(contrib.get("created_at"), '%Y-%m-%dT%H:%M:%SZ')
                            updated = True
                        except Exception as e:
                            print(f"Fehler beim Parsen des Erstellungsdatums für {contrib.get('login')}: {e}")
                    
                    if updated:
                        print(f"Contributor {contrib.get('login')} aktualisiert.")
                    else:
                        print(f"Contributor {contrib.get('login')} ist aktuell. Überspringe Aktualisierung.")
                    
                # Kleine Pause zur Schonung der API-Rate-Limits
                time.sleep(1)
                    
        except Exception as e:
            session.rollback()
            print(f"Fehler beim Aktualisieren der Details für Repository {repo_obj.name}: {e}")
    
    session.commit()
    print("Contributor-Daten wurden in der Datenbank aktualisiert.")

if __name__ == "__main__":
    update_contributors()