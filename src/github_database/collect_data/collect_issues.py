import requests
import time
import datetime
from dotenv import load_dotenv
from src.github_database.database.database import get_session, Repository, Issue
import os

# Lade Umgebungsvariablen und setze die API-Header
load_dotenv()
GITHUB_TOKEN = os.getenv('GITHUB_API_TOKEN')
headers = {
    'Authorization': f'token {GITHUB_TOKEN}',
    'Accept': 'application/vnd.github.v3+json'
}

def search_issues(owner_login, repo_name, per_page=100, max_pages=5, state="all"):
    """
    Ruft Issues eines Repositories über die GitHub API ab.
    
    Parameter:
      - owner_login: GitHub-Login des Repository-Besitzers
      - repo_name: Name des Repositories
      - per_page: Anzahl der Ergebnisse pro Seite (maximal 100)
      - max_pages: Maximale Seitenanzahl, die abgefragt wird
      - state: Issue-Status ('open', 'closed', 'all')
      
    Wichtig: Diese Funktion filtert Pull Requests heraus (da diese im Issue-Endpunkt enthalten sind).
    
    Gibt eine Liste von Issue-Dictionaries zurück.
    """
    issues = []
    for page in range(1, max_pages + 1):
        url = f"https://api.github.com/repos/{owner_login}/{repo_name}/issues?state={state}&per_page={per_page}&page={page}"
        print(f"Abfrage von Issues: Seite {page} für {owner_login}/{repo_name}")
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            if not data:
                print("Keine weiteren Issues gefunden.")
                break
            # Filtere Pull Requests heraus (diese enthalten ein "pull_request"-Feld)
            filtered_issues = [issue for issue in data if "pull_request" not in issue]
            issues.extend(filtered_issues)
            time.sleep(1)  # Pause zur Schonung der API-Rate-Limits
        else:
            print(f"Fehler beim Abrufen der Issues auf Seite {page} für {owner_login}/{repo_name}: HTTP {response.status_code}")
            break
    return issues

def create_issue_from_api(api_data, repository_internal_id):
    """
    Wandelt API-Daten eines Issues in ein Issue-Objekt um.
    
    Erfasste Felder:
      - github_id: Eindeutige Issue-ID (aus api_data["id"])
      - title: Issue-Titel
      - state: Issue-Status ("open" oder "closed")
      - created_at: Erstellungsdatum (als datetime)
      - updated_at: Letztes Update-Datum (als datetime)
      - author_id: GitHub-ID des Erstellers (aus dem "user"-Objekt)
    
    Der Parameter repository_internal_id verknüpft den Issue mit dem internen Repository-Eintrag.
    """
    github_id = api_data.get("id")
    title = api_data.get("title")
    state = api_data.get("state")
    
    created_at_str = api_data.get("created_at")
    updated_at_str = api_data.get("updated_at")
    created_at = None
    updated_at = None
    if created_at_str:
        try:
            created_at = datetime.datetime.strptime(created_at_str, '%Y-%m-%dT%H:%M:%SZ')
        except Exception as e:
            print(f"Fehler beim Parsen von created_at: {e}")
    if updated_at_str:
        try:
            updated_at = datetime.datetime.strptime(updated_at_str, '%Y-%m-%dT%H:%M:%SZ')
        except Exception as e:
            print(f"Fehler beim Parsen von updated_at: {e}")
    
    user_data = api_data.get("user")
    author_id = user_data.get("id") if user_data else None
    
    issue_obj = Issue(
        repo_id=repository_internal_id,
        github_id=github_id,
        title=title,
        state=state,
        created_at=created_at,
        updated_at=updated_at,
        author_id=author_id
    )
    return issue_obj

def update_issues():
    session = get_session()
    
    # Lade alle Repositories aus der Datenbank
    repositories = session.query(Repository).all()
    print(f"{len(repositories)} Repositories in der Datenbank gefunden.")
    
    for repo_obj in repositories:
        try:
            # Hole vollständige Repository-Daten, um Owner-Login und Repository-Namen zu erhalten
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
            
            # Abruf der Issues über die API
            issues_data = search_issues(owner_login, repo_name, per_page=100, max_pages=5, state="all")
            print(f"{len(issues_data)} Issues gefunden für Repository {repo_obj.name}")
            
            for issue_data in issues_data:
                existing_issue = session.query(Issue).filter_by(github_id=issue_data.get("id")).first()
                new_issue = create_issue_from_api(issue_data, repo_obj.id)
                if not existing_issue:
                    session.add(new_issue)
                    print(f"Issue {new_issue.github_id} für Repository {repo_obj.name} hinzugefügt.")
                else:
                    # Update-Logik: Aktualisiere relevante Felder, wenn sich diese geändert haben
                    updated = False
                    if new_issue.title != existing_issue.title:
                        existing_issue.title = new_issue.title
                        updated = True
                    if new_issue.state != existing_issue.state:
                        existing_issue.state = new_issue.state
                        updated = True
                    # Aktualisiere das Update-Datum, falls der neue Wert neuer ist
                    if new_issue.updated_at and (not existing_issue.updated_at or new_issue.updated_at > existing_issue.updated_at):
                        existing_issue.updated_at = new_issue.updated_at
                        updated = True
                    # Aktualisiere den author_id, falls vorhanden und unterschiedlich
                    if new_issue.author_id != existing_issue.author_id:
                        existing_issue.author_id = new_issue.author_id
                        updated = True
                    if updated:
                        print(f"Issue {existing_issue.github_id} für Repository {repo_obj.name} aktualisiert.")
                    else:
                        print(f"Issue {existing_issue.github_id} für Repository {repo_obj.name} ist aktuell. Überspringe.")
            time.sleep(1)
        except Exception as e:
            session.rollback()
            print(f"Fehler beim Verarbeiten der Issues für Repository {repo_obj.name}: {e}")
    
    session.commit()
    print("Issue-Daten wurden in der Datenbank aktualisiert.")

if __name__ == "__main__":
    update_issues()