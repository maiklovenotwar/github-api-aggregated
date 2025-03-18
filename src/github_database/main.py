import os
import time
from datetime import datetime, timedelta
import requests
from typing import Optional, Dict, List, Tuple
from sqlalchemy import inspect

# dotenv, falls du Umgebungsvariablen lädst
from dotenv import load_dotenv

# Eigene Module aus deinem Package "github_database"
from .api.github_api import get_repositories_since
from .database.database import (
    init_db,
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
    def __init__(self, threshold: int = 5):  # Niedrigerer Threshold für Core-API
        self.threshold = threshold
        self.remaining = None
        self.reset_time = None
        self._last_check = None
        self._check_interval = timedelta(minutes=1)  # Nur alle 1 Minute prüfen
        
    def should_check_rate_limit(self) -> bool:
        """Bestimmt, ob das Rate-Limit geprüft werden sollte"""
        if self._last_check is None:
            return True
        if self.remaining is not None and self.remaining < self.threshold:
            return True
        return datetime.now() - self._last_check > self._check_interval
        
    def check_rate_limit(self) -> None:
        """Überprüft das Core-API Rate-Limit nur wenn nötig"""
        if not self.should_check_rate_limit():
            return
            
        url = "https://api.github.com/rate_limit"
        response = requests.get(url, headers=headers)
        self._last_check = datetime.now()
        
        if response.status_code == 200:
            data = response.json()
            core = data.get("resources", {}).get("core", {})
            self.remaining = core.get("remaining", 0)
            reset = core.get("reset", 0)
            self.reset_time = datetime.fromtimestamp(reset)
            limit = core.get("limit", 5000)
            
            print(f"Core-API Rate-Limit: {self.remaining}/{limit} verbleibend (Reset: {self.reset_time})")
            
            if self.remaining < self.threshold:
                sleep_seconds = (self.reset_time - datetime.now()).total_seconds() + 5
                if sleep_seconds > 0:
                    print(f"Core-API Rate-Limit niedrig. Warte {int(sleep_seconds)} Sekunden...")
                    time.sleep(sleep_seconds)
        else:
            print(f"Fehler beim Abrufen des Rate-Limits: HTTP {response.status_code}")

def process_repositories(repositories: List[Dict], session) -> Tuple[int, int]:
    """
    Verarbeitet eine Liste von Repositories und speichert sie in der Datenbank.
    Nutzt SQLAlchemy Bulk-Operationen für bessere Performance.
    
    Returns:
        Tuple von (Anzahl neuer Repos, Anzahl aktualisierter Repos)
    """
    # Hole alle Spalten des Repository-Modells
    columns = [c.key for c in inspect(Repository).mapper.column_attrs]
    
    # Sammle neue und zu aktualisierende Repositories
    new_repos_dicts = []
    update_dicts = []
    existing_repo_ids = set(id_tuple[0] for id_tuple in 
                          session.query(Repository.repo_id)
                          .filter(Repository.repo_id.in_([r['id'] for r in repositories]))
                          .all())
    
    for repo_data in repositories:
        try:
            # Erstelle Repository-Objekt
            repo_obj = create_repository_from_api(repo_data)
            
            # Konvertiere zu Dictionary für Bulk-Operationen
            repo_dict = {attr: getattr(repo_obj, attr) for attr in columns}
            
            if repo_obj.repo_id in existing_repo_ids:
                # Füge nur die zu aktualisierenden Felder hinzu
                update_dict = {
                    'repo_id': repo_obj.repo_id,  # Primary Key für Update
                    'name': repo_obj.name,
                    'description': repo_obj.description,
                    'language': repo_obj.language,
                    'forks_count': repo_obj.forks_count,
                    'stars_count': repo_obj.stars_count,
                    'open_issues_count': repo_obj.open_issues_count,
                    'updated_at': datetime.utcnow()
                }
                update_dicts.append(update_dict)
            else:
                new_repos_dicts.append(repo_dict)
                
        except Exception as e:
            print(f"Fehler bei Repository {repo_data.get('full_name', 'unbekannt')}: {e}")
            continue
    
    try:
        # Bulk Insert für neue Repositories
        if new_repos_dicts:
            session.bulk_insert_mappings(Repository, new_repos_dicts)
        
        # Bulk Update für existierende Repositories
        if update_dicts:
            session.bulk_update_mappings(Repository, update_dicts)
        
        # Commit erst nach allen Operationen
        session.commit()
        
        return len(new_repos_dicts), len(update_dicts)
        
    except Exception as e:
        session.rollback()
        print(f"Fehler bei Bulk-Operation: {e}")
        return 0, 0

def main():
    # Initialisiere die Datenbank und Rate-Limit Manager
    init_db()
    rate_limit = RateLimitManager()
    
    # Öffne eine Datenbanksession
    session = get_session()
    
    try:
        # Überprüfe zu Beginn das Rate-Limit
        rate_limit.check_rate_limit()
        
        # Hole die letzte Repository ID aus der Datenbank
        last_repo = session.query(Repository).order_by(Repository.repo_id.desc()).first()
        since_id = last_repo.repo_id if last_repo else None
        
        total_new = 0
        total_updated = 0
        batch_count = 0
        max_batches = 50  # Begrenze auf 50 Batches pro Durchlauf
        
        while batch_count < max_batches:
            rate_limit.check_rate_limit()
            
            repositories, last_id = get_repositories_since(since_id)
            if not repositories or not last_id:
                print("Keine weiteren Repositories gefunden.")
                break
                
            new_count, updated_count = process_repositories(repositories, session)
            total_new += new_count
            total_updated += updated_count
            
            print(f"Batch {batch_count + 1}: {new_count} neue, {updated_count} aktualisierte Repositories")
            session.commit()
            
            since_id = last_id
            batch_count += 1
            
            # Kleine Pause zwischen den Batches
            time.sleep(1)
        
        print(f"\nVerarbeitung abgeschlossen: {total_new} neue Repositories hinzugefügt, {total_updated} aktualisiert")
        print(f"Letzte verarbeitete Repository ID: {since_id}")
        
    except Exception as e:
        session.rollback()
        print(f"Kritischer Fehler: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    main()