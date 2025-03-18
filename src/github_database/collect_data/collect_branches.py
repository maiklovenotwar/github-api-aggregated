import requests
import time
import datetime
from dotenv import load_dotenv
from src.github_database.database.database import get_session, Repository, Branch
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
    Ruft die vollständigen Details eines Repositories anhand der GitHub-spezifischen ID ab.
    Diese Daten enthalten u.a. den Owner-Login, der für den API-Aufruf der Branches benötigt wird.
    """
    url = f"https://api.github.com/repositories/{repo_id}"
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Fehler beim Abrufen der Repository-Details für {repo_id}: HTTP {response.status_code}")
        return None

def search_branches(owner_login, repo_name, per_page=100):
    """
    Ruft die Branches eines Repositories ab.
    Nutzt den Endpunkt: GET /repos/{owner}/{repo}/branches.
    """
    url = f"https://api.github.com/repos/{owner_login}/{repo_name}/branches?per_page={per_page}"
    print(f"Abfrage von Branches für {owner_login}/{repo_name}")
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Fehler beim Abrufen der Branches für {owner_login}/{repo_name}: HTTP {response.status_code}")
        return []

def create_branch_from_api(api_data, repository_internal_id, default_branch_name):
    """
    Wandelt API-Daten eines Branches in ein Branch-Objekt um.
    
    Erwartete Felder:
      - name: Der Name des Branches.
      - is_default: True, wenn der Branch-Name dem Standard-Branch entspricht.
      - created_at: Da GitHub den Erstellungszeitpunkt nicht liefert, setzen wir diesen auf None.
    """
    branch_name = api_data.get("name")
    is_default = (branch_name == default_branch_name)
    branch_obj = Branch(
        repo_id=repository_internal_id,
        name=branch_name,
        is_default=is_default,
        created_at=None  # GitHub liefert keinen Erstellungszeitpunkt für Branches
    )
    return branch_obj

def update_branches():
    session = get_session()
    
    # Lade alle Repositories aus der Datenbank
    repositories = session.query(Repository).all()
    print(f"{len(repositories)} Repositories in der Datenbank gefunden.")
    
    for repo_obj in repositories:
        try:
            # Hole Repository-Details, um Owner-Login und Repo-Namen zu erhalten
            repo_details = get_repo_details(repo_obj.repo_id)
            if not repo_details:
                continue
            owner_login = repo_details.get("owner", {}).get("login")
            repo_name = repo_details.get("name")
            if not owner_login or not repo_name:
                print(f"Unvollständige Daten für Repository {repo_obj.repo_id}. Überspringe.")
                continue
            
            # Abruf der Branches über die API
            branches_data = search_branches(owner_login, repo_name, per_page=100)
            print(f"{len(branches_data)} Branches gefunden für Repository {repo_obj.name}")
            
            for branch_data in branches_data:
                branch_name = branch_data.get("name")
                # Prüfe, ob der Branch bereits in der DB existiert (eindeutig per repo_id und Branch-Name)
                existing_branch = session.query(Branch).filter_by(repo_id=repo_obj.id, name=branch_name).first()
                new_is_default = (branch_name == repo_obj.default_branch)
                if not existing_branch:
                    branch_obj = create_branch_from_api(branch_data, repo_obj.id, repo_obj.default_branch)
                    session.add(branch_obj)
                    print(f"Branch '{branch_name}' für Repository {repo_obj.name} hinzugefügt.")
                else:
                    # Update-Logik: Aktualisiere is_default, falls sich dieser Wert geändert hat
                    if existing_branch.is_default != new_is_default:
                        existing_branch.is_default = new_is_default
                        print(f"Branch '{branch_name}' für Repository {repo_obj.name} aktualisiert (is_default geändert).")
                    else:
                        print(f"Branch '{branch_name}' existiert bereits für Repository {repo_obj.name}. Keine Aktualisierung notwendig.")
            time.sleep(1)  # Kurze Pause, um die Rate-Limits zu schonen
        except Exception as e:
            session.rollback()
            print(f"Fehler beim Verarbeiten der Branches für Repository {repo_obj.name}: {e}")
    
    session.commit()
    print("Branch-Daten wurden in der Datenbank aktualisiert.")

if __name__ == "__main__":
    update_branches()