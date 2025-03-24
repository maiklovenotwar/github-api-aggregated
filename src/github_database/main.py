"""Main entry point for GitHub data collection and aggregation."""

import logging
import os
import time
import argparse
from datetime import datetime, timezone, timedelta
from pathlib import Path

from .config import ETLConfig, GitHubConfig, BigQueryConfig
from .database.database import init_db
from .etl_orchestrator import ETLOrchestrator
from .aggregation import DataAggregator
from .optimized_collector import collect_repositories_parallel

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Main entry point."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="GitHub Data Collection and Aggregation")
    parser.add_argument("--mode", choices=["collect", "aggregate", "export"], default="collect",
                      help="Operation mode: collect data, aggregate data, or export data")
    parser.add_argument("--start-year", type=int, default=2015,
                      help="Start year for aggregation (default: 2015)")
    parser.add_argument("--end-year", type=int, default=datetime.now().year,
                      help=f"End year for aggregation (default: current year)")
    parser.add_argument("--output-dir", default="./data/exports",
                      help="Directory for exporting data (default: ./data/exports)")
    parser.add_argument("--max-bytes", type=int, default=None,
                      help="Maximum bytes billed for BigQuery queries (default: value from BIGQUERY_MAX_BYTES env var)")
    parser.add_argument("--limit", type=int, default=50,
                      help="Limit number of repositories to process (default: 50)")
    parser.add_argument("--min-stars", type=int, default=50,
                      help="Minimum stars for repositories (default: 50)")
    parser.add_argument("--min-forks", type=int, default=10,
                      help="Minimum forks for repositories (default: 10)")
    parser.add_argument("--parallel", action="store_true",
                      help="Use parallel processing for data collection")
    parser.add_argument("--workers", type=int, default=5,
                      help="Number of parallel workers for data collection (default: 5)")
    parser.add_argument("--batch-size", type=int, default=100,
                      help="Batch size for parallel processing (default: 100)")
    parser.add_argument("--tokens", nargs="+", default=None,
                      help="List of GitHub API tokens to use (default: use GITHUB_API_TOKEN from environment)")
    parser.add_argument("--reset-db", action="store_true",
                      help="Reset database before starting (default: False)")
    parser.add_argument("--time-period", default=None,
                      help="Time period for data collection (YYYY-MM), default is current month")
    
    args = parser.parse_args()
    
    # Debug-Ausgabe für Argumente
    logger.info(f"Command line arguments: mode={args.mode}, limit={args.limit}, min_stars={args.min_stars}, min_forks={args.min_forks}")
    if args.parallel:
        logger.info(f"Parallel processing enabled with {args.workers} workers and batch size {args.batch_size}")
        if args.tokens:
            token_count = len(args.tokens)
            logger.info(f"Using {token_count} GitHub API tokens")
    
    try:
        # Initialize configuration
        config = ETLConfig(
            database_url=os.getenv("DATABASE_URL", "sqlite:///github_data.db"),
            github=GitHubConfig(access_token=os.getenv("GITHUB_API_TOKEN", "")),
            bigquery=BigQueryConfig(
                project_id=os.getenv("BIGQUERY_PROJECT_ID", "github-api-archive"),
                dataset_id="githubarchive",
                table_id="day",
                credentials_path=Path(os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")),
                max_bytes_billed=int(os.getenv("BIGQUERY_MAX_BYTES", "1000000000"))
            )
        )
        
        # Setze die Parameter nach der Erstellung der ETLConfig-Instanz
        config.min_stars = args.min_stars
        config.min_forks = args.min_forks
        config.limit = args.limit
        
        # Zusätzliche Debug-Ausgabe für die Konfiguration
        logger.info(f"ETLConfig: min_stars={config.min_stars}, min_forks={config.min_forks}, limit={config.limit}")
        
        # Überschreibe max_bytes_billed, wenn angegeben
        if args.max_bytes:
            config.bigquery.max_bytes_billed = args.max_bytes
            logger.info(f"Setting max_bytes_billed to {args.max_bytes}")
        
        # Initialize database
        logger.info("Initializing database...")
        try:
            # Initialisiere Datenbank und erhalte Session-Factory
            Session = init_db(config.database_url, args.reset_db)
            # Erstelle Session
            session = Session()
            logger.info("Database initialized and session created successfully")
        except Exception as e:
            logger.error(f"Error initializing database: {e}")
            return
        
        # Ausführung basierend auf dem gewählten Modus
        if args.mode == "collect":
            # Datenerfassung über GitHub API
            if args.parallel:
                # Parallele Verarbeitung
                collect_data_parallel(config, Session, args.workers, args.batch_size, args.tokens, args.time_period)
            else:
                # Sequentielle Verarbeitung
                collect_data(config, session, args, args.time_period)
        elif args.mode == "aggregate":
            # Datenaggregation über BigQuery
            aggregate_data(config, session, args.start_year, args.end_year)
        elif args.mode == "export":
            # Datenexport
            export_data(config, session, args.output_dir)
        
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        if 'session' in locals():
            session.close()
            logger.info("Database session closed")

def collect_data_parallel(config, session_factory, max_workers=5, batch_size=100, tokens=None, time_period=None):
    """Collect data from GitHub API using parallel processing.
    
    Args:
        config: ETL configuration
        session_factory: Factory function for database sessions
        max_workers: Maximum number of parallel workers
        batch_size: Batch size for processing repositories
        tokens: List of GitHub API tokens to use
        time_period: Time period for data collection (YYYY-MM)
    """
    try:
        logger.info(f"Starting parallel data collection with {max_workers} workers and batch size {batch_size}")
        logger.info(f"Collecting up to {config.limit} repositories with at least {config.min_stars} stars and {config.min_forks} forks")
        
        # Verwende den optimierten parallelen Collector
        results = collect_repositories_parallel(
            config=config,
            session_factory=session_factory,
            max_workers=max_workers,
            batch_size=batch_size,
            tokens=tokens,
            time_period=time_period
        )
        
        # Zusammenfassung
        success_count = sum(1 for _, success in results if success)
        logger.info(f"Parallel data collection completed: {success_count}/{len(results)} repositories successfully processed")
        
    except Exception as e:
        logger.error(f"Error in parallel data collection: {e}")

def collect_data(config, session, args, time_period=None):
    """Collect data from GitHub API.
    
    Args:
        config: ETL configuration
        session: Database session
        args: Command line arguments
        time_period: Time period for data collection (YYYY-MM)
    """
    try:
        # Create ETL orchestrator
        # Wir benötigen die Session-Factory-Funktion, nicht die Session selbst
        # Da wir nur die Session haben, erstellen wir eine einfache Factory-Funktion
        session_factory = lambda: session
        orchestrator = ETLOrchestrator(config, session_factory)
        
        # Bestimme den Zeitraum für die Datensammlung
        # Wenn kein Zeitraum angegeben ist, verwenden wir den aktuellen Monat
        if not time_period:
            # Aktuelles Datum
            now = datetime.now()
            # Standardmäßig den aktuellen Monat verwenden
            year = now.year
            month = now.month
            time_period = f"{year}-{month:02d}"
        
        logger.info(f"Collecting repositories created in time period: {time_period}")
        
        # Get quality repositories using GitHub API
        logger.info(f"Getting repositories with at least {config.min_stars} stars and {config.min_forks} forks...")
        try:
            # Debug-Ausgabe für das Limit
            logger.info(f"DEBUG: Requested limit value is {config.limit}")
            
            # Verwende die optimierte Methode aus dem ETL-Orchestrator
            # Stelle sicher, dass das Limit korrekt verwendet wird
            logger.info(f"Fetching up to {config.limit} repositories with at least {config.min_stars} stars in batches of 100...")
            
            # Füge zusätzliche Debug-Ausgaben hinzu
            logger.info(f"Fetching batch 1 of {(config.limit + 99) // 100}...")
            
            repositories = orchestrator.get_quality_repositories(limit=config.limit, time_period=time_period)
            
            logger.info(f"Found {len(repositories)} quality repositories")
            
            if not repositories:
                logger.warning("No repositories found. Using fallback repositories.")
                # Fallback: Verwende einige bekannte populäre Repositories
                repositories = [
                    {"full_name": "microsoft/vscode"},
                    {"full_name": "facebook/react"},
                    {"full_name": "tensorflow/tensorflow"},
                    {"full_name": "kubernetes/kubernetes"},
                    {"full_name": "angular/angular"}
                ]
            
            # Process repositories
            logger.info(f"Processing {len(repositories)} repositories...")
            for i, repo_data in enumerate(repositories):
                try:
                    full_name = repo_data["full_name"]
                    logger.info(f"Processing repository {i+1}/{len(repositories)}: {full_name}")
                    
                    # Verarbeite das Repository
                    repository = orchestrator.process_repository(full_name, session)
                    
                    if repository:
                        logger.info(f"Successfully processed repository: {full_name}")
                    else:
                        logger.warning(f"Failed to process repository: {full_name}")
                    
                    # Commit nach jedem Repository, um Fortschritt zu speichern
                    session.commit()
                    
                except Exception as e:
                    logger.error(f"Error processing repository {repo_data.get('full_name', 'unknown')}: {e}")
                    session.rollback()
                
                # Kurze Pause, um API-Limits zu respektieren
                time.sleep(1)
            
            logger.info("All repositories processed successfully")
            
        except Exception as e:
            logger.error(f"Error processing repositories: {e}")
            session.rollback()
        
    except Exception as e:
        logger.error(f"Error in data collection: {e}")

def aggregate_data(config, session, start_year, end_year):
    """Aggregate data from BigQuery.
    
    Args:
        config: ETL configuration
        session: Database session
        start_year: Start year for aggregation
        end_year: End year for aggregation
    """
    try:
        logger.info(f"Starting data aggregation from {start_year} to {end_year}...")
        
        # Erstelle DataAggregator
        aggregator = DataAggregator(config, session)
        
        # Aggregiere Organisationsstatistiken in kleineren Zeitabschnitten
        logger.info("Aggregating organization statistics...")
        
        # Verarbeite Jahre in kleineren Blöcken, um Abfragelimits zu vermeiden
        max_years_per_block = 2
        for year_start in range(start_year, end_year + 1, max_years_per_block):
            year_end = min(year_start + max_years_per_block - 1, end_year)
            logger.info(f"Aggregating organization stats for years {year_start}-{year_end}...")
            
            try:
                aggregator.aggregate_organization_stats(year_start, year_end)
                logger.info(f"Successfully aggregated organization stats for years {year_start}-{year_end}")
            except Exception as e:
                logger.error(f"Error aggregating organization stats for years {year_start}-{year_end}: {e}")
                # Versuche einzelne Jahre, wenn ein Block fehlschlägt
                for year in range(year_start, year_end + 1):
                    try:
                        logger.info(f"Trying to aggregate organization stats for single year {year}...")
                        aggregator.aggregate_organization_stats(year, year)
                        logger.info(f"Successfully aggregated organization stats for year {year}")
                    except Exception as e2:
                        logger.error(f"Error aggregating organization stats for year {year}: {e2}")
        
        # Aggregiere Länderstatistiken in kleineren Zeitabschnitten
        logger.info("Aggregating country statistics...")
        
        for year_start in range(start_year, end_year + 1, max_years_per_block):
            year_end = min(year_start + max_years_per_block - 1, end_year)
            logger.info(f"Aggregating country stats for years {year_start}-{year_end}...")
            
            try:
                aggregator.aggregate_country_stats(year_start, year_end)
                logger.info(f"Successfully aggregated country stats for years {year_start}-{year_end}")
            except Exception as e:
                logger.error(f"Error aggregating country stats for years {year_start}-{year_end}: {e}")
                # Versuche einzelne Jahre, wenn ein Block fehlschlägt
                for year in range(year_start, year_end + 1):
                    try:
                        logger.info(f"Trying to aggregate country stats for single year {year}...")
                        aggregator.aggregate_country_stats(year, year)
                        logger.info(f"Successfully aggregated country stats for year {year}")
                    except Exception as e2:
                        logger.error(f"Error aggregating country stats for year {year}: {e2}")
        
        logger.info("Data aggregation completed successfully")
        
    except Exception as e:
        logger.error(f"Error in data aggregation: {e}")
        session.rollback()

def export_data(config, session, output_dir):
    """Export aggregated data to CSV files.
    
    Args:
        config: ETL configuration
        session: Database session
        output_dir: Directory for exporting data
    """
    try:
        logger.info(f"Exporting data to {output_dir}...")
        
        # Erstelle Ausgabeverzeichnis, falls es nicht existiert
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Erstelle DataAggregator
        aggregator = DataAggregator(config, session)
        
        # Exportiere Daten
        org_file = output_path / "organization_yearly_stats.csv"
        country_file = output_path / "country_yearly_stats.csv"
        
        logger.info(f"Exporting organization stats to {org_file}...")
        aggregator.export_organization_stats(str(org_file))
        
        logger.info(f"Exporting country stats to {country_file}...")
        aggregator.export_country_stats(str(country_file))
        
        logger.info("Data export completed successfully")
        
    except Exception as e:
        logger.error(f"Error in data export: {e}")


if __name__ == "__main__":
    main()