#!/usr/bin/env python3
"""
Sammelt diverse GitHub-Repositories über verschiedene Sternbereiche.

Dieses Skript verwendet den ETL-Orchestrator, um Repositories aus verschiedenen
Sternbereichen zu sammeln und in die Datenbank einzufügen, wobei für jeden Bereich
alle verfügbaren Repositories gesammelt werden, bevor zum nächsten übergegangen wird.
"""

import os
import sys
import time
import logging
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional

# Pfad zum Projektverzeichnis hinzufügen
project_dir = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_dir))

from src.github_database.etl.orchestrator import ETLOrchestrator
from src.github_database.database.database import GitHubDatabase
from src.github_database.config import load_config, ETLConfig

# Logging konfigurieren
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('collect_diverse_repos.log')
    ]
)

logger = logging.getLogger(__name__)

# Sternbereiche für diversifizierte Abfragen
STAR_RANGES = [
    (10, 50),       # Sehr kleine Projekte
    (50, 100),      # Kleine Projekte
    (100, 500),     # Mittelgroße Projekte
    (500, 1000),    # Größere Projekte
    (1000, 5000),   # Populäre Projekte
    (5000, 10000),  # Sehr populäre Projekte
    (10000, 50000), # Top-Projekte
    (50000, None)   # Super-Stars
]

# Sprachen für diversifizierte Abfragen
LANGUAGES = [
    "python",
    "javascript",
    "java",
    "go",
    "rust",
    "typescript",
    "c++",
    "c#",
    "php",
    "ruby"
]

def collect_repositories_for_range(
    orchestrator: ETLOrchestrator, 
    db: GitHubDatabase,
    min_stars: int,
    max_stars: Optional[int],
    language: str,
    min_forks: int = 0,
    batch_size: int = 100,
    max_batches: int = 10,
    with_activity_data: bool = True,
    created_after: Optional[str] = None,
    created_before: Optional[str] = None
) -> int:
    """
    Sammelt alle verfügbaren Repositories für einen bestimmten Sternbereich und Sprache.
    
    Args:
        orchestrator: ETL-Orchestrator-Instanz
        db: Datenbankverbindung
        min_stars: Minimale Anzahl von Sternen
        max_stars: Maximale Anzahl von Sternen (oder None für unbegrenzt)
        language: Programmiersprache
        min_forks: Mindestanzahl von Forks
        batch_size: Anzahl der Repositories pro Abfrage
        max_batches: Maximale Anzahl von Abfragen für diesen Bereich
        with_activity_data: Ob Aktivitätsdaten (Commits) abgerufen werden sollen
        created_after: Nur Repositories berücksichtigen, die nach diesem Datum erstellt wurden
        created_before: Nur Repositories berücksichtigen, die vor diesem Datum erstellt wurden
        
    Returns:
        Anzahl der neu gesammelten Repositories
    """
    logger.info(f"Sammle alle Repositories mit {min_stars}-{max_stars or 'unbegrenzt'} Sternen in {language}")
    
    # Bestehende Repository-IDs abrufen
    existing_ids = set(db.get_all_repository_ids())
    total_collected = 0
    
    # Für jeden Bereich explizit den Cache leeren
    orchestrator.github_client.clear_search_cache()
    
    # Eine große Anzahl von Repositories auf einmal abrufen
    # Die search_repositories-Methode führt die Paginierung intern durch
    try:
        repositories = orchestrator.github_client.search_repositories(
            min_stars=min_stars,
            max_stars=max_stars,
            min_forks=min_forks,
            language=language,
            limit=batch_size * max_batches,  # Holen wir uns mehr Repos auf einmal
            created_after=created_after,
            created_before=created_before,
            sort_by="stars",
            sort_order="desc"
        )
        
        if not repositories:
            logger.info(f"Keine Repositories gefunden für {min_stars}-{max_stars or 'unbegrenzt'} Sterne in {language}")
            return 0
        
        logger.info(f"Gefunden: {len(repositories)} Repositories")
        
        # Neue Repositories filtern
        new_repos = []
        for repo in repositories:
            if repo['id'] not in existing_ids:
                new_repos.append(repo)
                existing_ids.add(repo['id'])
        
        if new_repos:
            logger.info(f"Davon {len(new_repos)} neue Repositories")
            
            # Daten anreichern und in Datenbank einfügen
            for repo in new_repos:
                try:
                    # Aktivitätsdaten hinzufügen, wenn gewünscht
                    if with_activity_data:
                        try:
                            # Direktes Anreichern mit Commit-Daten
                            activity_data = orchestrator._collect_activity_data(repo['full_name'])
                            if activity_data:
                                repo.update(activity_data)
                        except Exception as e:
                            logger.warning(f"Fehler beim Abrufen der Aktivitätsdaten für {repo['full_name']}: {e}")
                    
                    db.insert_repository(repo)
                    total_collected += 1
                    
                    # Detaillierte Informationen loggen
                    commit_info = f", {repo.get('commits_last_year', 'N/A')} Commits im letzten Jahr" if 'commits_last_year' in repo else ""
                    logger.info(f"Eingefügt: {repo['full_name']} ({repo['stargazers_count']} Sterne, {repo['language']}{commit_info})")
                    
                except Exception as e:
                    logger.error(f"Fehler beim Einfügen von {repo.get('full_name', 'unbekannt')}: {e}")
        else:
            logger.info("Alle gefundenen Repositories sind bereits in der Datenbank")
    
    except Exception as e:
        logger.error(f"Fehler bei der Abfrage: {e}")
    
    # API-Statistiken für diesen Bereich ausgeben
    api_stats = orchestrator.github_client.get_api_statistics()
    logger.info(f"API-Statistiken für diesen Bereich: Anfragen: {api_stats.get('requests', 'N/A')}, " 
                f"Cache-Treffer: {api_stats.get('cache_hits', 'N/A')}")
    
    return total_collected

def collect_repositories(
    orchestrator: ETLOrchestrator, 
    db: GitHubDatabase, 
    total_target: int = 1000, 
    languages: Optional[List[str]] = None,
    min_forks: int = 0,
    with_activity_data: bool = True,
    created_after: Optional[str] = None
) -> int:
    """
    Sammelt Repositories über verschiedene Sternbereiche und Sprachen.
    
    Args:
        orchestrator: ETL-Orchestrator-Instanz
        db: Datenbankverbindung
        total_target: Zielanzahl der zu sammelnden Repositories
        languages: Liste der zu berücksichtigenden Sprachen
        min_forks: Mindestanzahl von Forks
        with_activity_data: Ob Aktivitätsdaten (Commits) abgerufen werden sollen
        created_after: Nur Repositories berücksichtigen, die nach diesem Datum erstellt wurden
        
    Returns:
        Anzahl der neu gesammelten Repositories
    """
    if not languages:
        languages = LANGUAGES
        
    total_collected = 0
    
    # Cache für die gesamte Abfrage leeren
    logger.info("Leere gesamten API-Cache für frische Ergebnisse")
    orchestrator.github_client.clear_caches()
    
    # Wenn created_after nicht angegeben, verwenden wir diverse Zeiträume für verschiedene Abfragen
    time_periods = []
    if created_after:
        time_periods.append((created_after, None))
    else:
        # Für die verschiedenen Zeiträume
        today = datetime.now()
        time_periods = [
            # Letzte 6 Monate
            ((today - timedelta(days=180)).strftime("%Y-%m-%d"), None),
            # 6-12 Monate
            ((today - timedelta(days=365)).strftime("%Y-%m-%d"), (today - timedelta(days=180)).strftime("%Y-%m-%d")),
            # 1-2 Jahre
            ((today - timedelta(days=730)).strftime("%Y-%m-%d"), (today - timedelta(days=365)).strftime("%Y-%m-%d")),
            # 2-5 Jahre
            ((today - timedelta(days=1825)).strftime("%Y-%m-%d"), (today - timedelta(days=730)).strftime("%Y-%m-%d"))
        ]
    
    # Zuerst über die Zeiträume, dann Sternbereiche, dann Sprachen
    for created_after, created_before in time_periods:
        if total_collected >= total_target:
            logger.info(f"Ziel von {total_target} Repositories erreicht, breche ab")
            break
            
        date_range_desc = f"zwischen {created_after} und {created_before}" if created_before else f"nach {created_after}"
        logger.info(f"Suche Repositories {date_range_desc}")
    
        # Iteration über Sternbereiche und Sprachen
        for min_stars, max_stars in STAR_RANGES:
            if total_collected >= total_target:
                logger.info(f"Ziel von {total_target} Repositories erreicht, breche ab")
                break
                
            for language in languages:
                if total_collected >= total_target:
                    break
                    
                # Für jeden Bereich ALLE verfügbaren Repositories sammeln
                collected = collect_repositories_for_range(
                    orchestrator=orchestrator,
                    db=db,
                    min_stars=min_stars,
                    max_stars=max_stars,
                    language=language,
                    min_forks=min_forks,
                    with_activity_data=with_activity_data,
                    created_after=created_after,
                    created_before=created_before
                )
                
                total_collected += collected
                logger.info(f"Insgesamt {total_collected} von {total_target} Repositories gesammelt")
                
                # Kurze Pause zwischen den Bereichen
                time.sleep(2)
    
    return total_collected

def main():
    """Hauptfunktion zum Sammeln diverser Repositories."""
    parser = argparse.ArgumentParser(description="Sammelt diverse GitHub-Repositories über verschiedene Sternbereiche")
    parser.add_argument("--total", type=int, default=1000, help="Zielanzahl der zu sammelnden Repositories")
    parser.add_argument("--languages", type=str, nargs="+", help="Zu berücksichtigende Sprachen")
    parser.add_argument("--created-after", type=str, help="Nur Repositories berücksichtigen, die nach diesem Datum erstellt wurden (YYYY-MM-DD)")
    parser.add_argument("--db-path", type=str, help="Pfad zur SQLite-Datenbank")
    parser.add_argument("--cache-dir", type=str, help="Verzeichnis für Cache-Dateien")
    parser.add_argument("--min-forks", type=int, default=0, help="Mindestanzahl von Forks (Standard: 0)")
    parser.add_argument("--no-activity-data", action="store_true", help="Keine Aktivitätsdaten (Commits) abrufen")
    args = parser.parse_args()
    
    # Konfiguration laden
    config = load_config()
    
    # ETL-Orchestrator initialisieren
    orchestrator = ETLOrchestrator(config, cache_dir=args.cache_dir)
    
    # Datenbank initialisieren - Standardwert auf github_data.db gesetzt
    db_path = args.db_path or os.environ.get("GITHUB_DB_PATH", "github_data.db")
    db = GitHubDatabase(db_path)
    
    # Startzeit für Leistungsmessung
    start_time = time.time()
    
    # Repositories sammeln
    total_collected = collect_repositories(
        orchestrator=orchestrator,
        db=db,
        total_target=args.total,
        languages=args.languages,
        min_forks=args.min_forks,
        with_activity_data=not args.no_activity_data,
        created_after=args.created_after
    )
    
    # Statistiken ausgeben
    elapsed_time = time.time() - start_time
    logger.info(f"Sammlung abgeschlossen: {total_collected} neue Repositories in {elapsed_time:.2f} Sekunden")
    if total_collected > 0:
        logger.info(f"Durchschnittliche Zeit pro Repository: {elapsed_time/total_collected:.2f} Sekunden")
    else:
        logger.info("Keine neuen Repositories gesammelt.")
    
    # ETL-Statistiken ausgeben
    stats = orchestrator.get_statistics()
    logger.info(f"ETL-Statistiken: {stats}")
    
    # Datenbank schließen
    db.close()

if __name__ == "__main__":
    main()