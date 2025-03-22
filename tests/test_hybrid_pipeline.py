"""Test the hybrid data collection pipeline (GitHub API + BigQuery)."""

import logging
import time
from datetime import datetime, timedelta
import psutil
import pandas as pd
from typing import List, Dict, Any
from pathlib import Path

from src.github_database.config import ETLConfig
from src.github_database.config.bigquery_config import BigQueryConfig
from src.github_database.api.github_api import get_repositories_since
from src.github_database.database.database import (
    init_db,
    get_session,
    Repository,
    Event
)
from src.github_database.etl_orchestrator import ETLOrchestrator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class PipelineMetrics:
    """Track performance metrics for the pipeline."""
    
    def __init__(self):
        self.start_time = time.time()
        self.api_duration = 0
        self.bigquery_duration = 0
        self.repositories_processed = 0
        self.events_by_type = {}
        self.memory_usage = []
        self.errors = []
        
    def track_memory(self):
        """Record current memory usage."""
        process = psutil.Process()
        memory_mb = process.memory_info().rss / 1024 / 1024
        self.memory_usage.append({
            'timestamp': datetime.now(),
            'memory_mb': memory_mb
        })
        
    def add_event(self, event_type: str):
        """Track event count by type."""
        self.events_by_type[event_type] = self.events_by_type.get(event_type, 0) + 1
        
    def log_error(self, component: str, error: Exception):
        """Log an error with component information."""
        self.errors.append({
            'timestamp': datetime.now(),
            'component': component,
            'error': str(error)
        })
        
    def get_summary(self) -> Dict[str, Any]:
        """Get summary of metrics."""
        return {
            'duration_seconds': time.time() - self.start_time,
            'api_duration_seconds': self.api_duration,
            'bigquery_duration_seconds': self.bigquery_duration,
            'repositories_processed': self.repositories_processed,
            'total_events': sum(self.events_by_type.values()),
            'events_by_type': self.events_by_type,
            'max_memory_mb': max(m['memory_mb'] for m in self.memory_usage) if self.memory_usage else 0,
            'error_count': len(self.errors)
        }
        
    def save_report(self, output_dir: Path):
        """Save metrics report to files."""
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Save summary
        summary = self.get_summary()
        pd.DataFrame([summary]).to_csv(
            output_dir / 'summary.csv',
            index=False
        )
        
        # Save memory usage
        pd.DataFrame(self.memory_usage).to_csv(
            output_dir / 'memory_usage.csv',
            index=False
        )
        
        # Save errors
        if self.errors:
            pd.DataFrame(self.errors).to_csv(
                output_dir / 'errors.csv',
                index=False
            )

def validate_repository_data(session, repository_ids: List[int]) -> bool:
    """
    Validate repository data in database.
    
    Args:
        session: Database session
        repository_ids: List of repository IDs to validate
        
    Returns:
        bool: True if validation passes
    """
    try:
        # Check if all repositories exist
        stored_repos = session.query(Repository).filter(
            Repository.id.in_(repository_ids)
        ).all()
        
        if len(stored_repos) != len(repository_ids):
            logger.error(f"Missing repositories. Expected {len(repository_ids)}, found {len(stored_repos)}")
            return False
            
        # Validate required fields
        for repo in stored_repos:
            if not all([
                repo.name,
                repo.full_name,
                repo.id,
                repo.created_at,
                repo.updated_at
            ]):
                logger.error(f"Missing required fields for repository {repo.id}")
                return False
                
        return True
        
    except Exception as e:
        logger.error(f"Repository validation error: {e}")
        return False

def validate_event_data(session, repository_ids: List[int]) -> bool:
    """
    Validate event data in database.
    
    Args:
        session: Database session
        repository_ids: List of repository IDs to validate
        
    Returns:
        bool: True if validation passes
    """
    try:
        # Check if events exist for repositories
        events = session.query(Event).filter(
            Event.repo_id.in_(repository_ids)
        ).all()
        
        if not events:
            logger.error("No events found for repositories")
            return False
            
        # Validate event data
        for event in events:
            # Check required fields
            if not all([
                event.event_id,
                event.repo_id,
                event.type,
                event.created_at
            ]):
                logger.error(f"Missing required fields for event {event.event_id}")
                return False
                
            # Validate timestamps
            if not isinstance(event.created_at, datetime):
                logger.error(f"Invalid timestamp for event {event.event_id}")
                return False
                
            # Validate repository relationship
            if event.repo_id not in repository_ids:
                logger.error(f"Event {event.event_id} references invalid repository")
                return False
                
        return True
        
    except Exception as e:
        logger.error(f"Event validation error: {e}")
        return False

def run_test_pipeline(
    start_date: datetime,
    end_date: datetime,
    max_repos: int = 5
) -> PipelineMetrics:
    """
    Run test pipeline for hybrid data collection.
    
    Args:
        start_date: Start date for data collection
        end_date: End date for data collection
        max_repos: Maximum number of repositories to process
        
    Returns:
        PipelineMetrics: Collection of pipeline metrics
    """
    metrics = PipelineMetrics()
    
    try:
        # Initialize database
        session = init_db()
        
        # Initialize ETL orchestrator
        config = ETLConfig(start_date=start_date, end_date=end_date)
        config.max_repositories = max_repos  # Limit number of repositories
        config.quality.min_stars = 1000  # Increase minimum stars to get fewer repositories
        
        orchestrator = ETLOrchestrator(config)
        
        # Get repositories from GitHub API
        api_start = time.time()
        repositories = get_repositories_since(
            since_date=start_date,
            max_repos=max_repos,
            min_stars=config.quality.min_stars
        )
        metrics.api_duration = time.time() - api_start
        
        if not repositories:
            raise ValueError("No repositories found")
            
        repository_ids = [repo['id'] for repo in repositories]
        
        # Process repositories
        orchestrator.process_repositories(
            repositories=repositories,
            start_date=start_date,
            end_date=end_date
        )
        metrics.repositories_processed = len(repositories)
        
        # Validate repository data
        if not validate_repository_data(session, repository_ids):
            raise ValueError("Repository validation failed")
            
        # Track final memory usage
        metrics.track_memory()
        
        session.close()
        return metrics
        
    except Exception as e:
        metrics.log_error("pipeline", e)
        logger.error(f"Pipeline error: {e}")
        return metrics

def test_pipeline():
    """Test the hybrid pipeline."""
    try:
        logger.info("Starting test pipeline...")
        
        # Set test parameters
        start_date = datetime(2024, 3, 1)  # Just query one day
        end_date = datetime(2024, 3, 1)
        min_stars = 1000  # Increase minimum stars to get fewer repositories
        max_repos = 5  # Limit number of repositories
        
        # Initialize ETL orchestrator
        config = ETLConfig(start_date=start_date, end_date=end_date)
        config.quality.min_stars = min_stars
        config.max_repositories = max_repos
        
        orchestrator = ETLOrchestrator(config)
        
        # Run pipeline
        orchestrator.run_pipeline()
        
        # Get metrics
        metrics = orchestrator.get_metrics()
        
        # Log results
        logger.info("Pipeline test completed:")
        logger.info(f"- Duration: {metrics.duration:.2f} seconds")
        logger.info(f"- Repositories: {metrics.repositories}")
        logger.info(f"- Total events: {metrics.events}")
        logger.info(f"- Memory peak: {metrics.memory_peak:.2f} MB")
        logger.info(f"- Errors: {metrics.errors}")
        
        # Analyze results
        # analyze_results(metrics)
        
    except Exception as e:
        logger.error(f"Pipeline error: {e}")
        raise

def main():
    """Run test pipeline and generate report."""
    logger.info("Starting test pipeline...")
    
    # Set test parameters
    start_date = datetime(2024, 1, 1)  # Use January 1st, 2024
    end_date = datetime(2024, 1, 1)
    min_stars = 10000  # Use more popular repositories
    max_repos = 100  # Increase number of repositories to find more events
    
    # Initialize ETL orchestrator
    config = ETLConfig(start_date=start_date, end_date=end_date)
    config.quality.min_stars = min_stars
    config.max_repositories = max_repos
    
    # Get repositories from GitHub API
    repositories = get_repositories_since(
        since_date=None,  # Don't filter by date to get older repositories
        max_repos=max_repos,
        min_stars=config.quality.min_stars
    )
    
    # Log repository IDs
    if repositories:
        logger.info("Repository IDs:")
        for repo in repositories[:5]:
            logger.info(f"- {repo['id']}: {repo['full_name']}")
    
    # Initialize orchestrator
    orchestrator = ETLOrchestrator(config)
    
    # Run pipeline
    orchestrator.run_pipeline()
    
    # Get metrics
    metrics = orchestrator.get_metrics()
    
    # Log results
    logger.info("Pipeline test completed:")
    logger.info(f"- Duration: {metrics.duration:.2f} seconds")
    logger.info(f"- Repositories: {metrics.repositories}")
    logger.info(f"- Total events: {metrics.events}")
    logger.info(f"- Memory peak: {metrics.memory_peak:.2f} MB")
    logger.info(f"- Errors: {metrics.errors}")
    
    # Analyze results
    # analyze_results(metrics)
    
    return metrics

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
