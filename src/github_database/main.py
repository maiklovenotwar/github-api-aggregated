"""Main entry point for GitHub data collection."""

import logging
import os
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

from .config import ETLConfig, GitHubConfig, BigQueryConfig
from .database.database import init_db
from .etl_orchestrator import ETLOrchestrator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Main entry point."""
    try:
        # Initialize configuration
        config = ETLConfig(
            database_url=os.getenv("DATABASE_URL", "sqlite:///github.db"),
            github=GitHubConfig(access_token=os.getenv("GITHUB_API_TOKEN", "")),
            bigquery=BigQueryConfig(
                project_id=os.getenv("BIGQUERY_PROJECT_ID", "github-api-archive"),
                dataset_id="githubarchive",
                table_id="day",
                credentials_path=Path(os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")),
                max_bytes_billed=int(os.getenv("BIGQUERY_MAX_BYTES", "1000000000"))
            )
        )
        
        # Initialize database
        logger.info("Initializing database...")
        try:
            # Initialisiere Datenbank und erhalte Session-Factory
            Session = init_db(config.database_url)
            # Erstelle Session
            session = Session()
            logger.info("Database initialized and session created successfully")
        except Exception as e:
            logger.error(f"Error initializing database: {e}")
            return
        
        # Create ETL orchestrator
        try:
            orchestrator = ETLOrchestrator(config)
        except Exception as e:
            logger.error(f"Error creating ETL orchestrator: {e}")
            return
        
        # Get repositories with at least 10 stars using GitHub API
        logger.info("Getting repositories with at least 10 stars...")
        try:
            # Verwende die GitHub API anstelle von BigQuery, um Repositories mit historischen Stars zu finden
            total_repos = 50  # Reduziert von 500 auf 50 für schnellere Testausführung
            batch_size = 100    # Größe jedes Batches
            all_repositories = []
            
            logger.info(f"Fetching {total_repos} repositories with at least 10 stars in batches of {batch_size}...")
            
            for i in range(0, total_repos, batch_size):
                logger.info(f"Fetching batch {i//batch_size + 1} of {(total_repos + batch_size - 1)//batch_size}...")
                batch_repositories = orchestrator.github_client.search_repositories(
                    min_stars=10,  # Gemäß den Quality Thresholds
                    limit=batch_size
                )
                
                if not batch_repositories:
                    logger.warning(f"No more repositories found after {len(all_repositories)} repositories.")
                    break
                
                all_repositories.extend(batch_repositories)
                logger.info(f"Fetched {len(all_repositories)} repositories so far.")
                
                # Wenn wir genug Repositories haben, brechen wir ab
                if len(all_repositories) >= total_repos:
                    all_repositories = all_repositories[:total_repos]
                    break
                
                # Kurze Pause, um API-Limits zu respektieren
                time.sleep(1)
            
            repositories = all_repositories
            logger.info(f"Found a total of {len(repositories)} repositories with at least 10 stars")
            
            if not repositories:
                logger.warning("No repositories found. Using fallback repositories.")
                # Fallback: Verwende einige bekannte populäre Repositories
                repositories = [
                    {"full_name": "microsoft/vscode", "stars": 100, "contributors": 0, "commits": 0},
                    {"full_name": "facebook/react", "stars": 100, "contributors": 0, "commits": 0},
                    {"full_name": "tensorflow/tensorflow", "stars": 100, "contributors": 0, "commits": 0},
                    {"full_name": "kubernetes/kubernetes", "stars": 100, "contributors": 0, "commits": 0},
                    {"full_name": "flutter/flutter", "stars": 100, "contributors": 0, "commits": 0}
                ]
                
        except Exception as e:
            logger.error(f"Error getting repositories: {e}")
            # Fallback: Verwende einige bekannte populäre Repositories
            repositories = [
                {"full_name": "microsoft/vscode", "stars": 100, "contributors": 0, "commits": 0},
                {"full_name": "facebook/react", "stars": 100, "contributors": 0, "commits": 0},
                {"full_name": "tensorflow/tensorflow", "stars": 100, "contributors": 0, "commits": 0},
                {"full_name": "kubernetes/kubernetes", "stars": 100, "contributors": 0, "commits": 0},
                {"full_name": "flutter/flutter", "stars": 100, "contributors": 0, "commits": 0}
            ]
        
        # Process each repository
        for repo_data in repositories:
            try:
                logger.info(f"Processing repository {repo_data['full_name']}...")
                try:
                    repo = orchestrator.process_repository(repo_data['full_name'], session)
                except Exception as e:
                    logger.error(f"Error processing repository {repo_data['full_name']}: {e}")
                    continue
                
                if repo:
                    # Update metrics from BigQuery
                    try:
                        metrics = orchestrator.bigquery_client.get_repository_metrics(
                            full_name=repo_data['full_name'],
                            since=datetime.now(timezone.utc) - timedelta(days=365)
                        )
                    except Exception as e:
                        logger.error(f"Error getting repository metrics for {repo_data['full_name']}: {e}")
                        metrics = {'stars': repo_data.get('stars', 0), 'contributors': 0, 'commits': 0}
                    
                    repo.stars = metrics['stars']
                    repo.contributors = metrics['contributors']
                    repo.commits = metrics['commits']
                    
                    # Get events
                    try:
                        events = orchestrator.bigquery_client.get_repository_events(
                            full_name=repo_data['full_name'],
                            since=datetime.now(timezone.utc) - timedelta(days=365),
                            batch_size=1000  # Aus den Quality Thresholds
                        )
                    except Exception as e:
                        logger.error(f"Error getting repository events for {repo_data['full_name']}: {e}")
                        events = []
                    
                    logger.info(f"Found {len(events)} events for {repo_data['full_name']}")
                    
                    # Process events in batches
                    for event_data in events:
                        try:
                            orchestrator._process_event(event_data, session)
                        except Exception as e:
                            logger.error(f"Error processing event: {e}")
                            continue
                    
                    try:
                        session.commit()
                    except Exception as e:
                        logger.error(f"Error committing session: {e}")
                        session.rollback()
                        continue
                    
                    logger.info(f"Successfully processed {repo_data['full_name']}")
                    
            except Exception as e:
                logger.error(f"Error processing repository {repo_data['full_name']}: {e}")
                continue
    
    except Exception as e:
        logger.error(f"Error running main script: {e}")

if __name__ == "__main__":
    main()