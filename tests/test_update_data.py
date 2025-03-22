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

def get_repositories_by_ids(start_id: int, end_id: int, per_page: int = 100) -> List[Dict]:
    """
    Ruft Repositories in einem bestimmten ID-Bereich ab.
    
    Args:
        start_id: Start-ID für die Abfrage
        end_id: End-ID für die Abfrage
        per_page: Anzahl der Repositories pro Anfrage (max. 100)
        
    Returns:
        Liste der gefundenen Repositories
    """
    repositories = []
    current_id = start_id
    
    while current_id <= end_id:
        url = f"https://api.github.com/repositories"
        params = {
            "since": current_id - 1,  # -1 weil die API ">since" zurückgibt
            "per_page": per_page
        }
        
        response = make_github_request(url, params)
        batch = response.json()
        
        if not batch:
            break
            
        # Nur Repositories im gewünschten Bereich behalten
        valid_repos = [repo for repo in batch if start_id <= repo['id'] <= end_id]
        repositories.extend(valid_repos)
        print(f"Abgerufen: {len(valid_repos)} Repositories im Bereich {start_id}-{end_id}")
        
        # Nächste ID setzen
        current_id = batch[-1]['id'] + 1
        
        # Wenn wir über das Ende hinaus sind, abbrechen
        if current_id > end_id:
            break
            
        # Kleine Pause zwischen den Anfragen
        time.sleep(1)
    
    return repositories

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
                    'id': repo_obj.repo_id,  # Primary Key für Update
                    'name': repo_obj.name,
                    'description': repo_obj.description,
                    'language': repo_obj.language,
                    'forks_count': repo_obj.forks_count,
                    'stars_count': repo_obj.stars_count,
                    'open_issues_count': repo_obj.open_issues_count,
                    'updated_at': datetime.now()  # Nutze timezone-aware UTC Zeit
                }
                update_dicts.append(update_dict)
            else:
                # Setze die ID für neue Repositories
                repo_dict['id'] = repo_obj.repo_id
                new_repos_dicts.append(repo_dict)
                
        except Exception as e:
            print(f"Fehler bei Repository {repo_data.get('full_name', 'unbekannt')}: {e}")
            continue
    
    try:
        # Bulk Insert für neue Repositories
        if new_repos_dicts:
            session.bulk_insert_mappings(Repository, new_repos_dicts)
            print(f"Neue Repositories eingefügt: {len(new_repos_dicts)}")
        
        # Bulk Update für existierende Repositories
        if update_dicts:
            # Führe Updates einzeln durch, um Fehler zu vermeiden
            for update_dict in update_dicts:
                session.query(Repository).filter(Repository.id == update_dict['id']).update(
                    {k: v for k, v in update_dict.items() if k != 'id'}
                )
            print(f"Repositories aktualisiert: {len(update_dicts)}")
        
        # Commit erst nach allen Operationen
        session.commit()
        
        return len(new_repos_dicts), len(update_dicts)
        
    except Exception as e:
        session.rollback()
        print(f"Fehler bei Bulk-Operation: {e}")
        return 0, 0

def test_update_repositories(start_id: int = 63, end_id: int = 369):
    """
    Führt einen Update-Test für einen bestimmten ID-Bereich durch.
    Standardmäßig werden die ersten 100 Repositories aus dem ersten Durchlauf aktualisiert.
    
    Args:
        start_id: Start-ID für die Aktualisierung
        end_id: End-ID für die Aktualisierung
    """
    # Initialisiere Datenbank
    db = Database()
    db.create_tables()
    session = db.get_session()
    
    try:
        # Überprüfe zu Beginn das Rate-Limit
        rate_limit = RateLimitManager()
        rate_limit.check_rate_limit()
        
        print(f"\n=== Starte Update-Test für IDs {start_id}-{end_id} ===\n")
        
        # Hole die aktuellen Daten für die Repositories
        repositories = get_repositories_by_ids(start_id, end_id)
        
        if repositories:
            new_count, updated_count = process_repositories(repositories, session)
            
            print(f"\n=== Update-Test abgeschlossen ===")
            print(f"Neue Repositories: {new_count}")
            print(f"Aktualisierte Repositories: {updated_count}")
            print(f"Verarbeitete IDs: {start_id}-{end_id}")
        else:
            print("Keine Repositories gefunden.")
            
    except Exception as e:
        session.rollback()
        print(f"Kritischer Fehler: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    import sys
    
    # Standardwerte für start_id und end_id
    start_id = 63
    end_id = 369
    
    # Verarbeite Kommandozeilenargumente
    if len(sys.argv) > 2:
        start_id = int(sys.argv[1])
        end_id = int(sys.argv[2])
    
    # Update die Repositories im angegebenen ID-Bereich
    test_update_repositories(start_id, end_id)
