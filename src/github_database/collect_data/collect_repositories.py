from datetime import timedelta
# Importiere die Suchfunktion aus dem API-Modul
from ..api.github_api import search_repositories
# Importiere die Datenbankfunktionen und das Repository-Modell aus dem database-Modul
from ..database.database import init_db, get_session, create_repository_from_api, Repository

def collect_repositories():
    # Initialisiere die Datenbank (erstellt alle Tabellen, falls noch nicht vorhanden)
    init_db()
    
    # Öffne eine Datenbanksession
    session = get_session()
    
    # Ermittele das zuletzt gespeicherte Repository anhand des "created_at"-Datums
    last_repo = session.query(Repository).order_by(Repository.created_at.desc()).first()
    if last_repo and last_repo.created_at:
        # Ziehe einen Tag zurück, um sicherzustellen, dass auch Repositories erfasst werden,
        # die denselben Zeitstempel haben wie der letzte Eintrag.
        start_date = last_repo.created_at - timedelta(days=1)
        query = f"created:>{start_date.strftime('%Y-%m-%dT%H:%M:%SZ')}"
        print(f"Abfrage neuer Repositories, erstellt nach: {start_date}")
    else:
        # Falls noch keine Daten vorhanden sind, benutze einen Standardzeitraum für den gesamten Zeitraum.
        # Wir nehmen hier den 1. Januar 2008 als Startdatum.
        default_start = "2008-01-01T00:00:00Z"
        query = f"created:>{default_start}"
        print("Keine vorhandenen Repositories gefunden. Abfrage für den gesamten Zeitraum seit 2008.")
    
    # Abruf der Repositories über die Search API
    repositories = search_repositories(query, per_page=100, max_pages=10)
    print("Insgesamt gefundene Repositories:", len(repositories))
    
    # Verarbeitung der abgerufenen Repositories
    for repo_data in repositories:
        try:
            # Wenn die API-Daten als Dictionary vorliegen, konvertiere sie in ein Repository-Objekt
            if isinstance(repo_data, dict):
                repo_obj = create_repository_from_api(repo_data)
            else:
                repo_obj = repo_data  # Falls bereits ein DB-Objekt vorliegt
                
            # Überprüfe, ob das Repository schon in der DB existiert
            existing_repo = session.query(Repository).filter_by(repo_id=repo_obj.repo_id).first()
            if not existing_repo:
                session.add(repo_obj)
                print(f"Repository {repo_obj.name} hinzugefügt.")
            else:
                # Update-Logik: Aktualisiere Felder, die sich ändern können
                existing_repo.name = repo_obj.name
                existing_repo.owner = repo_obj.owner
                # created_at bleibt in der Regel konstant, kann aber bei Bedarf auch aktualisiert werden.
                existing_repo.updated_at = repo_obj.updated_at
                existing_repo.description = repo_obj.description
                existing_repo.language = repo_obj.language
                existing_repo.default_branch = repo_obj.default_branch
                existing_repo.forks_count = repo_obj.forks_count
                existing_repo.stars_count = repo_obj.stars_count
                existing_repo.open_issues_count = repo_obj.open_issues_count
                print(f"Repository {repo_obj.name} existiert bereits. Daten aktualisiert.")
        except Exception as e:
            session.rollback()
            print("Fehler beim Verarbeiten eines Repositories:", e)
    
    session.commit()
    print("Repository-Daten wurden in der Datenbank gespeichert.")

if __name__ == "__main__":
    collect_repositories()