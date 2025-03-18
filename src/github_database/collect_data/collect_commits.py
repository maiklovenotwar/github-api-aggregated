import requests
import time
import datetime
from dotenv import load_dotenv
from src.github_database.database.database import get_session, Repository, Commit
import os

# Lade die Umgebungsvariablen und setze den API-Header
load_dotenv()
GITHUB_TOKEN = os.getenv('GITHUB_API_TOKEN')
headers = {
    'Authorization': f'token {GITHUB_TOKEN}',
    'Accept': 'application/vnd.github.v3+json'
}

def get_repo_details(repo_id):
    """
    Ruft die vollständigen Details eines Repositories anhand der GitHub-spezifischen ID ab.
    Diese Daten enthalten u.a. den Owner-Login, der für die Commits-Abfrage benötigt wird.
    """
    url = f"https://api.github.com/repositories/{repo_id}"
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Fehler beim Abrufen von Repository-Details für {repo_id}: HTTP {response.status_code}")
        return None

def search_commits(owner_login, repo_name, per_page=100, max_pages=5):
    """
    Ruft die Commits eines Repositories über die GitHub API ab.
    Paginierung: Es werden bis zu max_pages Ergebnisseiten abgefragt.
    """
    commits = []
    for page in range(1, max_pages + 1):
        url = f"https://api.github.com/repos/{owner_login}/{repo_name}/commits?per_page={per_page}&page={page}"
        print(f"Abfrage von Commits: Seite {page} für {owner_login}/{repo_name}")
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            if not data:
                print("Keine weiteren Commits gefunden.")
                break
            commits.extend(data)
            time.sleep(1)  # Kurze Pause zur Schonung der Rate-Limits
        else:
            print(f"Fehler beim Abrufen der Commits auf Seite {page} für {owner_login}/{repo_name}: HTTP {response.status_code}")
            break
    return commits

def create_commit_from_api(api_data, repository_internal_id):
    """
    Wandelt API-Daten eines Commits in ein Commit-Objekt um.
    
    Erfasst:
      - commit_hash: SHA-Hash des Commits.
      - message: Commit-Nachricht.
      - committed_at: Datum/Uhrzeit des Commits (als datetime).
      - author_id: GitHub-ID des Autors (sofern vorhanden).
    """
    commit_hash = api_data.get("sha")
    commit_message = api_data.get("commit", {}).get("message")
    committed_at_str = api_data.get("commit", {}).get("author", {}).get("date")
    committed_at = None
    if committed_at_str:
        try:
            committed_at = datetime.datetime.strptime(committed_at_str, '%Y-%m-%dT%H:%M:%SZ')
        except Exception as e:
            print(f"Fehler beim Parsen des Commit-Datums: {e}")
    author = api_data.get("author")
    author_id = author.get("id") if author else None
    
    commit_obj = Commit(
        repo_id=repository_internal_id,
        commit_hash=commit_hash,
        message=commit_message,
        committed_at=committed_at,
        author_id=author_id
    )
    return commit_obj

def update_commits():
    session = get_session()
    
    # Lade alle Repositories aus der Datenbank
    repositories = session.query(Repository).all()
    print(f"{len(repositories)} Repositories in der Datenbank gefunden.")
    
    for repo_obj in repositories:
        try:
            # Hole die vollständigen Repository-Daten, um Owner-Login und Repo-Namen zu erhalten
            repo_details = get_repo_details(repo_obj.repo_id)
            if not repo_details:
                continue
            owner_login = repo_details.get("owner", {}).get("login")
            repo_name = repo_details.get("name")
            if not owner_login or not repo_name:
                print(f"Unvollständige Daten für repo_id {repo_obj.repo_id}. Überspringe.")
                continue
            
            # Abruf der Commits über die API
            commits_data = search_commits(owner_login, repo_name, per_page=100, max_pages=5)
            print(f"{len(commits_data)} Commits gefunden für Repository {repo_name}")
            
            for commit_data in commits_data:
                commit_hash = commit_data.get("sha")
                # Suche nach einem bestehenden Commit in der DB anhand des commit_hash
                existing_commit = session.query(Commit).filter_by(commit_hash=commit_hash).first()
                new_commit_obj = create_commit_from_api(commit_data, repo_obj.id)
                if not existing_commit:
                    session.add(new_commit_obj)
                    print(f"Commit {commit_hash} hinzugefügt.")
                else:
                    # Update-Logik: Falls sich relevante Felder geändert haben, aktualisiere sie.
                    updated = False
                    if existing_commit.message != new_commit_obj.message:
                        existing_commit.message = new_commit_obj.message
                        updated = True
                    if existing_commit.committed_at != new_commit_obj.committed_at:
                        existing_commit.committed_at = new_commit_obj.committed_at
                        updated = True
                    if existing_commit.author_id != new_commit_obj.author_id:
                        existing_commit.author_id = new_commit_obj.author_id
                        updated = True
                    if updated:
                        print(f"Commit {commit_hash} aktualisiert.")
                    else:
                        print(f"Commit {commit_hash} existiert bereits und ist aktuell. Überspringe.")
            time.sleep(1)
        except Exception as e:
            session.rollback()
            print(f"Fehler beim Verarbeiten der Commits für Repository {repo_obj.name}: {e}")
    
    session.commit()
    print("Commit-Daten wurden in der Datenbank aktualisiert.")

if __name__ == "__main__":
    update_commits()