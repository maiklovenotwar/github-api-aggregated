#!/usr/bin/env python3
"""
Skript zum Abfragen und Analysieren der SQLite-Datenbank mit GitHub-Repositories.
"""

import os
import sys
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import func, desc, and_, or_
from sqlalchemy.orm import Session

# Füge das Projekt-Verzeichnis zum Python-Pfad hinzu
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.github_database.database.database import init_db, Repository, User, Organization, Event

# Konfiguriere Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_top_repositories(session: Session, limit: int = 10) -> List[Repository]:
    """
    Gibt die Top-Repositories nach Stars zurück.
    
    Args:
        session: SQLAlchemy-Session
        limit: Anzahl der zurückzugebenden Repositories
        
    Returns:
        Liste von Repository-Objekten
    """
    return session.query(Repository).order_by(Repository.stars.desc()).limit(limit).all()

def get_repositories_by_language(session: Session, language: str, limit: int = 10) -> List[Repository]:
    """
    Gibt Repositories nach Programmiersprache zurück.
    
    Args:
        session: SQLAlchemy-Session
        language: Programmiersprache
        limit: Anzahl der zurückzugebenden Repositories
        
    Returns:
        Liste von Repository-Objekten
    """
    return session.query(Repository).filter(Repository.language == language).order_by(Repository.stars.desc()).limit(limit).all()

def get_repositories_by_organization(session: Session, org_login: str) -> List[Repository]:
    """
    Gibt Repositories einer bestimmten Organisation zurück.
    
    Args:
        session: SQLAlchemy-Session
        org_login: Login-Name der Organisation
        
    Returns:
        Liste von Repository-Objekten
    """
    org = session.query(Organization).filter(Organization.login == org_login).first()
    if not org:
        return []
    return org.repositories

def get_top_organizations(session: Session, limit: int = 10) -> List[Organization]:
    """
    Gibt die Top-Organisationen nach Anzahl der Repositories zurück.
    
    Args:
        session: SQLAlchemy-Session
        limit: Anzahl der zurückzugebenden Organisationen
        
    Returns:
        Liste von Organization-Objekten
    """
    return session.query(Organization, func.count(Repository.id).label('repo_count')) \
        .join(Repository, Repository.organization_id == Organization.id) \
        .group_by(Organization.id) \
        .order_by(desc('repo_count')) \
        .limit(limit) \
        .all()

def get_language_distribution(session: Session) -> Dict[str, int]:
    """
    Gibt die Verteilung der Programmiersprachen zurück.
    
    Args:
        session: SQLAlchemy-Session
        
    Returns:
        Dictionary mit Programmiersprachen und Anzahl der Repositories
    """
    results = session.query(Repository.language, func.count(Repository.id).label('count')) \
        .filter(Repository.language.isnot(None)) \
        .group_by(Repository.language) \
        .order_by(desc('count')) \
        .all()
    
    return {lang: count for lang, count in results}

def plot_language_distribution(language_dist: Dict[str, int], top_n: int = 10):
    """
    Erstellt ein Balkendiagramm der Programmiersprachen-Verteilung.
    
    Args:
        language_dist: Dictionary mit Programmiersprachen und Anzahl der Repositories
        top_n: Anzahl der anzuzeigenden Top-Sprachen
    """
    # Top N Sprachen auswählen
    top_languages = sorted(language_dist.items(), key=lambda x: x[1], reverse=True)[:top_n]
    languages, counts = zip(*top_languages)
    
    plt.figure(figsize=(12, 8))
    plt.bar(languages, counts)
    plt.title(f'Top {top_n} Programmiersprachen nach Anzahl der Repositories')
    plt.xlabel('Programmiersprache')
    plt.ylabel('Anzahl der Repositories')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig('language_distribution.png')
    plt.close()
    
    logger.info(f"Diagramm gespeichert als 'language_distribution.png'")

def plot_stars_distribution(session: Session):
    """
    Erstellt ein Histogramm der Stars-Verteilung.
    
    Args:
        session: SQLAlchemy-Session
    """
    stars = [repo.stars for repo in session.query(Repository.stars).all()]
    
    plt.figure(figsize=(12, 8))
    plt.hist(stars, bins=50)
    plt.title('Verteilung der Stars')
    plt.xlabel('Anzahl der Stars')
    plt.ylabel('Anzahl der Repositories')
    plt.yscale('log')  # Logarithmische Skala für bessere Lesbarkeit
    plt.tight_layout()
    plt.savefig('stars_distribution.png')
    plt.close()
    
    logger.info(f"Diagramm gespeichert als 'stars_distribution.png'")

def export_to_csv(session: Session, query_result, filename: str):
    """
    Exportiert Abfrageergebnisse als CSV-Datei.
    
    Args:
        session: SQLAlchemy-Session
        query_result: Abfrageergebnis
        filename: Name der CSV-Datei
    """
    if isinstance(query_result[0], Repository):
        data = [{
            'id': repo.id,
            'full_name': repo.full_name,
            'description': repo.description,
            'language': repo.language,
            'stars': repo.stars,
            'forks': repo.forks,
            'created_at': repo.created_at,
            'updated_at': repo.updated_at
        } for repo in query_result]
    elif isinstance(query_result[0], tuple) and isinstance(query_result[0][0], Organization):
        data = [{
            'id': org.id,
            'login': org.login,
            'name': org.name,
            'location': org.location,
            'repo_count': repo_count
        } for org, repo_count in query_result]
    else:
        logger.error(f"Unbekannter Ergebnistyp: {type(query_result[0])}")
        return
    
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False)
    logger.info(f"Daten exportiert als '{filename}'")

def main():
    """Hauptfunktion zum Abfragen und Analysieren der Datenbank."""
    try:
        # Datenbank-URL aus Umgebungsvariable oder Standardwert
        database_url = os.getenv("DATABASE_URL", "sqlite:///github_data.db")
        
        # Datenbank initialisieren
        logger.info(f"Verbinde mit Datenbank: {database_url}")
        Session = init_db(database_url)
        session = Session()
        
        # Beispielabfragen
        logger.info("Führe Beispielabfragen durch...")
        
        # 1. Top 10 Repositories nach Stars
        top_repos = get_top_repositories(session, limit=10)
        logger.info("Top 10 Repositories nach Stars:")
        for repo in top_repos:
            logger.info(f"  - {repo.full_name}: {repo.stars} Stars, {repo.language}")
        
        # 2. Programmiersprachen-Verteilung
        language_dist = get_language_distribution(session)
        logger.info("Top 5 Programmiersprachen:")
        for lang, count in sorted(language_dist.items(), key=lambda x: x[1], reverse=True)[:5]:
            logger.info(f"  - {lang}: {count} Repositories")
        
        # 3. Top 10 Organisationen
        top_orgs = get_top_organizations(session, limit=10)
        logger.info("Top 10 Organisationen nach Anzahl der Repositories:")
        for org, repo_count in top_orgs:
            logger.info(f"  - {org.login}: {repo_count} Repositories")
        
        # 4. Repositories nach Programmiersprache (z.B. Python)
        python_repos = get_repositories_by_language(session, "Python", limit=10)
        logger.info("Top 10 Python-Repositories:")
        for repo in python_repos:
            logger.info(f"  - {repo.full_name}: {repo.stars} Stars")
        
        # 5. Repositories einer bestimmten Organisation (z.B. Microsoft)
        microsoft_repos = get_repositories_by_organization(session, "microsoft")
        logger.info(f"Microsoft-Repositories: {len(microsoft_repos)}")
        for repo in microsoft_repos[:5]:  # Zeige nur die ersten 5
            logger.info(f"  - {repo.full_name}: {repo.stars} Stars, {repo.language}")
        
        # Visualisierungen erstellen
        logger.info("Erstelle Visualisierungen...")
        plot_language_distribution(language_dist)
        plot_stars_distribution(session)
        
        # Daten exportieren
        logger.info("Exportiere Daten...")
        export_to_csv(session, top_repos, "top_repositories.csv")
        export_to_csv(session, top_orgs, "top_organizations.csv")
        
        logger.info("Analyse abgeschlossen.")
        
    except Exception as e:
        logger.error(f"Fehler bei der Datenbankabfrage: {e}")

if __name__ == "__main__":
    main()
