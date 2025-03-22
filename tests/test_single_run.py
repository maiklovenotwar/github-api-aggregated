import os
import time
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple
import requests
from sqlalchemy import inspect
from dotenv import load_dotenv
from src.github_database.api.github_api import make_github_request
from src.github_database.database.database import (
    Database,
    get_session,
    create_repository_from_api,
    Repository
)

# Lade Umgebungsvariablen
load_dotenv()
GITHUB_TOKEN = os.getenv('GITHUB_API_TOKEN')
headers = {
    'Authorization': f'token {GITHUB_TOKEN}',
    'Accept': 'application/vnd.github.v3+json'
}

class RateLimitManager:
    """Rate limit manager for GitHub API."""
    
    def __init__(self, threshold: int = 5):  # Niedrigerer Threshold für Core-API
        self.threshold = threshold
        self.remaining = None
        self.reset_time = None
        self._last_check = None
        self._check_interval = timedelta(minutes=1)  # Nur alle 1 Minute prüfen
        
    def should_check_rate_limit(self) -> bool:
        """Bestimmt, ob das Rate-Limit geprüft werden sollte."""
        if self._last_check is None:
            return True
        return datetime.now() - self._last_check > self._check_interval
    
    def check_rate_limit(self):
        """Überprüft das Core-API Rate-Limit nur wenn nötig."""
        if not self.should_check_rate_limit():
            return
            
        response = make_github_request('rate_limit')
        if response.ok:
            data = response.json()
            self.remaining = data['resources']['core']['remaining']
            self.reset_time = datetime.fromtimestamp(data['resources']['core']['reset'])
            self._last_check = datetime.now()
            
            if self.remaining <= self.threshold:
                wait_time = (self.reset_time - datetime.now()).total_seconds()
                if wait_time > 0:
                    print(f"Rate limit niedrig ({self.remaining}). Warte {wait_time:.0f} Sekunden...")
                    time.sleep(wait_time + 1)  # +1 Sekunde Sicherheit

def get_single_batch_repositories(since_id: Optional[int] = None, per_page: int = 100) -> Tuple[List[Dict], Optional[int]]:
    """
    Ruft nur einen einzelnen Batch von Repositories ab.
    
    Args:
        since_id: Repository ID, ab der die Abfrage starten soll
        per_page: Anzahl der Repositories pro Anfrage (max. 100)
        
    Returns:
        Tuple aus (Liste der gefundenen Repositories, letzte Repository ID)
    """
    params = {
        'per_page': per_page,
        'sort': 'updated',
        'direction': 'desc'
    }
    if since_id:
        params['since'] = since_id
        
    response = make_github_request('repositories', params=params)
    if not response.ok:
        print(f"Fehler beim Abrufen der Repositories: {response.status_code}")
        return [], None
        
    repositories = response.json()
    if not repositories:
        return [], None
        
    last_id = repositories[-1]['id'] if repositories else None
    return repositories, last_id

def process_repositories(repositories: List[Dict], session) -> Tuple[int, int]:
    """
    Verarbeitet eine Liste von Repositories und speichert sie in der Datenbank.
    Nutzt SQLAlchemy Bulk-Operationen für bessere Performance.
    
    Returns:
        Tuple von (Anzahl neuer Repos, Anzahl aktualisierter Repos)
    """
    if not repositories:
        return 0, 0
        
    # Sammle alle Repository IDs
    repo_ids = [repo['id'] for repo in repositories]
    
    # Prüfe, welche Repositories bereits existieren
    existing_repos = {
        repo.id: repo for repo in 
        session.query(Repository).filter(Repository.id.in_(repo_ids)).all()
    }
    
    new_repos = []
    updated_repos = []
    
    for repo_data in repositories:
        repo_id = repo_data['id']
        
        if repo_id in existing_repos:
            # Repository existiert bereits - prüfe auf Updates
            existing_repo = existing_repos[repo_id]
            updated_at = datetime.strptime(repo_data['updated_at'], '%Y-%m-%dT%H:%M:%SZ')
            
            if existing_repo.updated_at != updated_at:
                # Aktualisiere existierendes Repository
                for key, value in repo_data.items():
                    if hasattr(existing_repo, key):
                        setattr(existing_repo, key, value)
                updated_repos.append(existing_repo)
        else:
            # Neues Repository
            new_repo = create_repository_from_api(repo_data)
            new_repos.append(new_repo)
    
    # Bulk Insert für neue Repositories
    if new_repos:
        session.bulk_save_objects(new_repos)
    
    # Commit Änderungen
    try:
        session.commit()
        return len(new_repos), len(updated_repos)
    except Exception as e:
        session.rollback()
        print(f"Fehler beim Speichern der Repositories: {e}")
        return 0, 0

def test_single_run():
    """
    Führt einen einzelnen Durchlauf durch (1 Batch = 100 Repositories).
    """
    # Initialisiere Datenbank
    db = Database()
    db.create_tables()
    session = db.get_session()
    
    try:
        # Initialisiere Rate Limit Manager
        rate_limit_manager = RateLimitManager()
        
        # Prüfe und warte ggf. auf Rate Limit
        rate_limit_manager.check_rate_limit()
        
        # Hole einen Batch Repositories
        repositories, last_id = get_single_batch_repositories()
        
        if repositories:
            # Verarbeite Repositories
            new_count, updated_count = process_repositories(repositories, session)
            print(f"Verarbeitet: {len(repositories)} Repositories")
            print(f"Neu: {new_count}, Aktualisiert: {updated_count}")
            print(f"Letzte Repository ID: {last_id}")
            
            # Prüfe Datenbankstatus
            inspector = inspect(session.get_bind())
            tables = inspector.get_table_names()
            print(f"\nDatenbank-Tabellen: {tables}")
            
            for table in tables:
                count = session.execute(f"SELECT COUNT(*) FROM {table}").scalar()
                print(f"Anzahl Einträge in {table}: {count}")
    finally:
        session.close()

if __name__ == "__main__":
    test_single_run()
