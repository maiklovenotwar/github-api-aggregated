import requests
import time
import datetime
from dotenv import load_dotenv
from src.github_database.database.database import get_session, Repository, PullRequest
import os

# Lade Umgebungsvariablen und setze API-Header
load_dotenv()
GITHUB_TOKEN = os.getenv('GITHUB_API_TOKEN')
headers = {
    'Authorization': f'token {GITHUB_TOKEN}',
    'Accept': 'application/vnd.github.v3+json'
}

def search_pull_requests(owner_login, repo_name, per_page=100, max_pages=5, state='all'):
    """
    Ruft Pull Requests eines Repositories ab.
    
    Parameter:
      - owner_login: GitHub-Login des Repository-Besitzers
      - repo_name: Name des Repositories
      - per_page: Anzahl der Ergebnisse pro Seite (maximal 100)
      - max_pages: Maximale Anzahl an Seiten
      - state: PR-Status ('open', 'closed', 'all')
      
    Gibt eine Liste von Pull Request-Dictionaries zurück.
    """
    prs = []
    for page in range(1, max_pages + 1):
        url = f"https://api.github.com/repos/{owner_login}/{repo_name}/pulls?state={state}&per_page={per_page}&page={page}"
        print(f"Abfrage von Pull Requests: Seite {page} für {owner_login}/{repo_name}")
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            if not data:
                print("Keine weiteren Pull Requests gefunden.")
                break
            prs.extend(data)
            time.sleep(1)  # Kurze Pause zum Schonen der API-Rate-Limits
        else:
            print(f"Fehler beim Abrufen der PRs auf Seite {page} für {owner_login}/{repo_name}: HTTP {response.status_code}")
            break
    return prs

def create_pull_request_from_api(api_data, repository_internal_id):
    """
    Wandelt API-Daten eines Pull Requests in ein PullRequest-Objekt um.
    
    Erwartete Felder:
      - github_id: Eindeutige PR-ID (aus api_data.get("id"))
      - title: PR-Titel
      - state: PR-Status (z.B. "open", "closed", "merged")
      - created_at: Erstellungsdatum (als datetime)
      - merged_at: Merge-Datum (als datetime, falls vorhanden, sonst None)
      - author_id: Die GitHub-ID des Erstellers (aus dem "user"-Objekt)
    """
    pr_id = api_data.get("id")
    title = api_data.get("title")
    state = api_data.get("state")
    
    created_at_str = api_data.get("created_at")
    merged_at_str = api_data.get("merged_at")
    
    created_at = None
    merged_at = None
    if created_at_str:
        try:
            created_at = datetime.datetime.strptime(created_at_str, '%Y-%m-%dT%H:%M:%SZ')
        except Exception as e:
            print(f"Fehler beim Parsen von created_at: {e}")
    if merged_at_str:
        try:
            merged_at = datetime.datetime.strptime(merged_at_str, '%Y-%m-%dT%H:%M:%SZ')
        except Exception as e:
            print(f"Fehler beim Parsen von merged_at: {e}")
    
    user_data = api_data.get("user")
    author_id = user_data.get("id") if user_data else None
    
    pr_obj = PullRequest(
        repo_id=repository_internal_id,  # Verknüpfe den PR mit dem internen Repository-Eintrag
        github_id=pr_id,
        title=title,
        state=state,
        created_at=created_at,
        merged_at=merged_at,
        author_id=author_id
    )
    return pr_obj

def update_pull_requests():
    session = get_session()
    
    # Lade alle Repositories aus der Datenbank
    repositories = session.query(Repository).all()
    print(f"{len(repositories)} Repositories in der Datenbank gefunden.")
    
    for repo_obj in repositories:
        try:
            # Hole die vollständigen Repository-Daten, um Owner-Login und Repository-Namen zu erhalten
            url = f"https://api.github.com/repositories/{repo_obj.repo_id}"
            response = requests.get(url, headers=headers)
            if response.status_code != 200:
                print(f"Fehler beim Abrufen der Repository-Daten für {repo_obj.name}: HTTP {response.status_code}")
                continue
            repo_details = response.json()
            owner_login = repo_details.get("owner", {}).get("login")
            repo_name = repo_details.get("name")
            if not owner_login or not repo_name:
                print(f"Unvollständige Daten für Repository {repo_obj.repo_id}. Überspringe.")
                continue
            
            # Abruf der Pull Requests über die API
            prs_data = search_pull_requests(owner_login, repo_name, per_page=100, max_pages=5, state="all")
            print(f"{len(prs_data)} Pull Requests gefunden für Repository {repo_obj.name}")
            
            for pr_data in prs_data:
                # Prüfe, ob der Pull Request bereits in der Datenbank existiert (basierend auf github_id)
                existing_pr = session.query(PullRequest).filter_by(github_id=pr_data.get("id")).first()
                if not existing_pr:
                    pr_obj = create_pull_request_from_api(pr_data, repo_obj.id)
                    session.add(pr_obj)
                    print(f"Pull Request {pr_data.get('id')} hinzugefügt.")
                else:
                    # Update-Logik: Aktualisiere Felder, die sich ändern können
                    updated = False
                    # Aktualisiere Titel
                    new_title = pr_data.get("title")
                    if new_title and new_title != existing_pr.title:
                        existing_pr.title = new_title
                        updated = True
                    # Aktualisiere Status
                    new_state = pr_data.get("state")
                    if new_state and new_state != existing_pr.state:
                        existing_pr.state = new_state
                        updated = True
                    # Aktualisiere merged_at
                    merged_at_str = pr_data.get("merged_at")
                    new_merged_at = None
                    if merged_at_str:
                        try:
                            new_merged_at = datetime.datetime.strptime(merged_at_str, '%Y-%m-%dT%H:%M:%SZ')
                        except Exception as e:
                            print(f"Fehler beim Parsen von merged_at: {e}")
                    if new_merged_at != existing_pr.merged_at:
                        existing_pr.merged_at = new_merged_at
                        updated = True
                    if updated:
                        print(f"Pull Request {pr_data.get('id')} aktualisiert.")
                    else:
                        print(f"Pull Request {pr_data.get('id')} ist aktuell. Überspringe Update.")
            time.sleep(1)  # Kurze Pause, um Rate-Limits zu schonen
        except Exception as e:
            session.rollback()
            print(f"Fehler beim Verarbeiten der Pull Requests für Repository {repo_obj.name}: {e}")
    
    session.commit()
    print("Pull Request-Daten wurden in der Datenbank aktualisiert.")

if __name__ == "__main__":
    update_pull_requests()