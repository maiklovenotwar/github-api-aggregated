"""Optimierter Datensammler für GitHub-Repositories mit paralleler Verarbeitung."""

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Optional, Tuple
from sqlalchemy.orm import Session

from .etl_orchestrator import ETLOrchestrator
from .config import ETLConfig
from .api.token_pool import TokenPool

logger = logging.getLogger(__name__)

class OptimizedCollector:
    """Optimierter Datensammler für GitHub-Repositories mit paralleler Verarbeitung."""
    
    def __init__(
        self, 
        config: ETLConfig, 
        session_factory, 
        token_pool=None, 
        workers: int = 5, 
        batch_size: int = 100,
        time_period: Optional[str] = None
    ):
        """
        Initialisiert den optimierten Datensammler.
        
        Args:
            config: ETL-Konfiguration
            session_factory: Factory-Funktion für Datenbank-Sessions
            token_pool: Optional, Token-Pool für GitHub API
            workers: Anzahl der parallelen Worker
            batch_size: Größe der Batches für Datenbankoperationen
            time_period: Zeitraum für die Datensammlung (Format: YYYY-MM)
        """
        self.config = config
        self.session_factory = session_factory
        self.token_pool = token_pool
        self.workers = workers
        self.batch_size = batch_size
        self.time_period = time_period
        self.orchestrator = ETLOrchestrator(config, session_factory, token_pool)
    
    def collect_repositories(self, limit: int) -> List[Tuple[str, bool]]:
        """
        Sammelt Repository-Daten parallel mit mehreren Workern.
        
        Args:
            limit: Maximale Anzahl der zu sammelnden Repositories
            
        Returns:
            Liste von Tupeln (Repository-Name, Erfolg)
        """
        # Aktualisiere das Limit in der Konfiguration
        self.config.limit = limit
        
        # Hole qualitativ hochwertige Repositories
        logger.info(f"Suche nach Repositories mit mindestens {self.config.min_stars} Sternen...")
        repositories = self.orchestrator.get_quality_repositories(limit=limit, time_period=self.time_period)
        logger.info(f"{len(repositories)} Repositories gefunden")
        
        if not repositories:
            logger.warning("Keine Repositories gefunden")
            return []
        
        # Verarbeite Repositories parallel
        results = []
        
        def process_repo(repo_data: Dict[str, Any]) -> Tuple[str, bool]:
            """Verarbeite ein einzelnes Repository."""
            session = self.session_factory()
            try:
                full_name = repo_data["full_name"]
                logger.info(f"Verarbeite Repository: {full_name}")
                
                # Verarbeite Repository
                repository = self.orchestrator.process_repository(full_name, session)
                
                if repository:
                    logger.info(f"Repository erfolgreich verarbeitet: {full_name}")
                    session.commit()
                    return full_name, True
                else:
                    logger.warning(f"Fehler beim Verarbeiten des Repositories: {full_name}")
                    session.rollback()
                    return full_name, False
                    
            except Exception as e:
                logger.error(f"Fehler beim Verarbeiten des Repositories {repo_data.get('full_name', 'unbekannt')}: {e}")
                session.rollback()
                return repo_data.get('full_name', 'unbekannt'), False
            finally:
                session.close()
        
        # Verarbeite Repositories in Batches, um Speicherverbrauch zu kontrollieren
        for i in range(0, len(repositories), self.batch_size):
            batch = repositories[i:i+self.batch_size]
            logger.info(f"Verarbeite Batch {i//self.batch_size + 1}/{(len(repositories) + self.batch_size - 1)//self.batch_size} mit {len(batch)} Repositories")
            
            batch_results = []
            with ThreadPoolExecutor(max_workers=self.workers) as executor:
                future_to_repo = {executor.submit(process_repo, repo): repo for repo in batch}
                for future in as_completed(future_to_repo):
                    repo = future_to_repo[future]
                    try:
                        name, success = future.result()
                        batch_results.append((name, success))
                    except Exception as e:
                        logger.error(f"Unerwarteter Fehler bei Repository {repo.get('full_name', 'unbekannt')}: {e}")
            
            results.extend(batch_results)
            
            # Zeige Token-Pool-Statistiken, falls vorhanden
            if self.token_pool:
                stats = self.token_pool.get_stats()
                logger.info(f"Token-Pool-Statistiken:")
                for stat in stats:
                    logger.info(f"  Token {stat['index']}: {stat['rate_limit_remaining']} Anfragen verbleibend, Reset in {stat['reset_in_seconds']:.1f}s")
            
            # Kurze Pause zwischen Batches
            if i + self.batch_size < len(repositories):
                logger.info("Kurze Pause zwischen Batches...")
                time.sleep(2)
        
        # Zusammenfassung
        success_count = sum(1 for _, success in results if success)
        logger.info(f"Datensammlung abgeschlossen: {success_count}/{len(results)} Repositories erfolgreich verarbeitet")
        
        return results

def collect_repositories_parallel(
    config: ETLConfig,
    session_factory,
    max_workers: int = 5,
    batch_size: int = 100,
    tokens: Optional[List[str]] = None,
    time_period: Optional[str] = None
) -> List[Tuple[str, bool]]:
    """
    Sammelt Repository-Daten parallel mit mehreren Workern.
    
    Args:
        config: ETL-Konfiguration
        session_factory: Factory-Funktion für Datenbank-Sessions
        max_workers: Maximale Anzahl paralleler Worker
        batch_size: Größe der Batches für Datenbankoperationen
        tokens: Liste von GitHub API-Tokens (optional)
        time_period: Zeitraum für die Datensammlung (Format: YYYY-MM)
        
    Returns:
        Liste von Tupeln (Repository-Name, Erfolg)
    """
    # Initialisiere Token-Pool, falls mehrere Tokens angegeben wurden
    token_pool = None
    if tokens and len(tokens) > 1:
        token_pool = TokenPool(tokens)
        logger.info(f"Token-Pool mit {len(tokens)} Tokens initialisiert")
    elif tokens and len(tokens) == 1:
        logger.info("Nur ein Token angegeben, verwende Standard-Konfiguration")
        # Überschreibe den Token in der Konfiguration
        config.github.access_token = tokens[0]
    
    # Erstelle ETL-Orchestrator mit Token-Pool
    orchestrator = ETLOrchestrator(config, session_factory, token_pool=token_pool)
    
    # Hole qualitativ hochwertige Repositories
    logger.info(f"Suche nach Repositories mit mindestens {config.min_stars} Sternen...")
    repositories = orchestrator.get_quality_repositories(limit=config.limit, time_period=time_period)
    logger.info(f"{len(repositories)} Repositories gefunden")
    
    if not repositories:
        logger.warning("Keine Repositories gefunden")
        return []
    
    # Verarbeite Repositories parallel
    results = []
    
    def process_repo(repo_data: Dict[str, Any]) -> Tuple[str, bool]:
        """Verarbeite ein einzelnes Repository."""
        session = session_factory()
        try:
            full_name = repo_data["full_name"]
            logger.info(f"Verarbeite Repository: {full_name}")
            
            # Verarbeite Repository
            repository = orchestrator.process_repository(full_name, session)
            
            if repository:
                logger.info(f"Repository erfolgreich verarbeitet: {full_name}")
                session.commit()
                return full_name, True
            else:
                logger.warning(f"Fehler beim Verarbeiten des Repositories: {full_name}")
                session.rollback()
                return full_name, False
                
        except Exception as e:
            logger.error(f"Fehler beim Verarbeiten des Repositories {repo_data.get('full_name', 'unbekannt')}: {e}")
            session.rollback()
            return repo_data.get('full_name', 'unbekannt'), False
        finally:
            session.close()
    
    # Verarbeite Repositories in Batches, um Speicherverbrauch zu kontrollieren
    for i in range(0, len(repositories), batch_size):
        batch = repositories[i:i+batch_size]
        logger.info(f"Verarbeite Batch {i//batch_size + 1}/{(len(repositories) + batch_size - 1)//batch_size} mit {len(batch)} Repositories")
        
        batch_results = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_repo = {executor.submit(process_repo, repo): repo for repo in batch}
            for future in as_completed(future_to_repo):
                repo = future_to_repo[future]
                try:
                    name, success = future.result()
                    batch_results.append((name, success))
                except Exception as e:
                    logger.error(f"Unerwarteter Fehler bei Repository {repo.get('full_name', 'unbekannt')}: {e}")
        
        results.extend(batch_results)
        
        # Zeige Token-Pool-Statistiken, falls vorhanden
        if token_pool:
            stats = token_pool.get_stats()
            logger.info(f"Token-Pool-Statistiken:")
            for stat in stats:
                logger.info(f"  Token {stat['index']}: {stat['rate_limit_remaining']} Anfragen verbleibend, Reset in {stat['reset_in_seconds']:.1f}s")
        
        # Kurze Pause zwischen Batches
        if i + batch_size < len(repositories):
            logger.info("Kurze Pause zwischen Batches...")
            time.sleep(2)
    
    # Zusammenfassung
    success_count = sum(1 for _, success in results if success)
    logger.info(f"Datensammlung abgeschlossen: {success_count}/{len(results)} Repositories erfolgreich verarbeitet")
    
    return results
