"""ETL orchestration for GitHub data processing."""

import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Generator, Dict, Any, List
import gc

from .config import ETLConfig
from .config.bigquery_config import BigQueryConfig
from .database.database import (
    create_tables,
    get_session,
    create_repository_from_api,
    Repository
)
from .bigquery.bigquery_client import BigQueryClient
from .bigquery.event_parser import EventParser
from .processing.batch_processor import BatchProcessor
from .control_database.validate_data import DataValidator
from .enrichment.data_enricher import DataEnricher

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ETLOrchestrator:
    """Orchestrate the ETL process for GitHub data using both API and BigQuery."""
    
    def __init__(self, config: ETLConfig, bigquery_config: Optional[BigQueryConfig] = None):
        """
        Initialize ETL orchestrator.
        
        Args:
            config: ETL configuration
            bigquery_config: Optional BigQuery configuration
        """
        self.config = config
        self.bigquery_config = bigquery_config or BigQueryConfig.from_env()
        self.bigquery_config.max_bytes_billed = 20_000_000_000  # 20GB
        
        # Initialize components
        self.bigquery_client = BigQueryClient(self.bigquery_config)
        self.event_parser = EventParser()
        
        self.batch_processor = BatchProcessor(
            config=config,
            batch_size=config.batch_size
        )
        
        self.data_validator = DataValidator()
        self.data_enricher = DataEnricher(config, config.cache_dir)
        
        # Ensure database tables exist
        create_tables()
        
    def process_repositories(
        self,
        repositories: List[Dict],
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ):
        """
        Process GitHub data for repositories.
        
        Args:
            repositories: List of repository data from GitHub API
            start_date: Optional start date (inclusive)
            end_date: Optional end date (inclusive)
        """
        try:
            # Step 1: Store repositories in database
            session = get_session()
            repository_ids = []
            
            for repo_data in repositories:
                try:
                    # Check if repository already exists
                    repo = session.query(Repository).filter_by(id=repo_data['id']).first()
                    if not repo:
                        repo = create_repository_from_api(repo_data)
                        session.add(repo)
                    repository_ids.append(repo.id)
                except Exception as e:
                    logger.error(f"Error processing repository {repo_data.get('id')}: {e}")
                    
            session.commit()
            
            # Step 2: Process events from BigQuery if dates provided
            if start_date and end_date:
                self._process_historical_events(
                    repository_ids,
                    start_date,
                    end_date
                )
            
        finally:
            # Ensure all remaining events are processed
            self.batch_processor.flush_queues()
            session.close()
            
    def _process_historical_events(
        self,
        repository_ids: List[int],
        start_date: datetime,
        end_date: datetime
    ):
        """
        Process historical events for repositories.
        
        Args:
            repository_ids: List of repository IDs to process
            start_date: Start date for event collection
            end_date: End date for event collection
        """
        # Process events in batches to manage memory
        for repository_batch in self._batch_repositories(repository_ids):
            events = self.bigquery_client.get_events(
                start_date=start_date,
                end_date=end_date,
                repository_ids=repository_batch
            )
            
            for event in events:
                self.batch_processor.add_event(event)
                    
            # Force garbage collection after each batch
            gc.collect()
            
    def _batch_repositories(self, repository_ids: List[int]) -> Generator[List[int], None, None]:
        """Generate batches of repository IDs."""
        batch = []
        for repository_id in repository_ids:
            batch.append(repository_id)
            if len(batch) >= self.config.batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

    def get_metrics(self):
        """Get pipeline metrics."""
        return self.batch_processor.get_metrics()

    def run_pipeline(self):
        """Run the ETL pipeline."""
        try:
            # Get repositories from GitHub API
            from .api.github_api import get_repositories_since
            
            repositories = get_repositories_since(
                since_date=self.config.start_date,
                min_stars=self.config.quality.min_stars,
                max_repos=self.config.max_repositories
            )
            
            # Process repositories
            self.process_repositories(
                repositories,
                start_date=self.config.start_date,
                end_date=self.config.end_date
            )
            
        except Exception as e:
            logger.error(f"Pipeline error: {e}")
            raise
