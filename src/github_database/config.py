"""Configuration management for GitHub data collection."""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Set, Optional, Dict
from pathlib import Path

@dataclass
class QualityThresholds:
    """Repository quality thresholds."""
    min_stars: int = 50
    min_forks: int = 10
    min_commits_last_year: int = 100
    languages: Optional[Set[str]] = None

@dataclass
class APIConfig:
    """GitHub API configuration."""
    token: str = ""
    requests_per_hour: int = 5000
    min_remaining_rate: int = 100
    retry_wait_time: int = 60

@dataclass
class BigQueryConfig:
    """BigQuery configuration for GitHub Archive data."""
    project_id: str = ""
    dataset_id: str = "githubarchive"
    table_prefix: str = "github_timeline"
    credentials_path: Optional[Path] = None
    max_bytes_billed: int = 20_000_000_000  # 20GB
    batch_size: int = 1000
    max_query_days: int = 30  # Maximum days to query in a single batch

@dataclass
class ArchiveConfig:
    """GitHub Archive configuration."""
    cache_dir: Path = Path("cache/github_archive")
    batch_size: int = 1000
    max_daily_events: int = 1000000
    parallel_downloads: int = 5

@dataclass
class DatabaseConfig:
    """Database configuration."""
    url: str = "sqlite:///github_database.db"
    batch_size: int = 500
    max_retries: int = 3
    retry_delay: int = 5

@dataclass
class ProcessingState:
    """Track processing progress for resume capability."""
    last_processed_date: datetime = field(default_factory=lambda: datetime.now())
    last_processed_hour: int = 0
    processed_repo_ids: Set[int] = field(default_factory=set)
    failed_repo_ids: Set[int] = field(default_factory=set)
    event_counts: Dict[str, int] = field(default_factory=dict)

class ETLConfig:
    """Master configuration for ETL process."""
    
    def __init__(self, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None):
        """
        Initialize with default values.
        
        Args:
            start_date: Optional start date for data collection
            end_date: Optional end date for data collection
        """
        self.start_date = start_date
        self.end_date = end_date
        
        self.quality = QualityThresholds()
        self.api = APIConfig()
        self.bigquery = BigQueryConfig()
        self.archive = ArchiveConfig()
        self.database = DatabaseConfig()
        self.state = ProcessingState()
        
        # General settings
        self.cache_dir = Path("cache")
        self.batch_size = 1000
        self.max_retries = 3
        self.state_file = Path("etl_state.json")
        self.max_repositories = None  # Maximum number of repositories to process
    
    @classmethod
    def from_env(cls, env_file: Path = Path(".env"), start_date: Optional[datetime] = None, end_date: Optional[datetime] = None):
        """
        Create configuration from environment variables.
        
        Args:
            env_file: Path to .env file
            start_date: Optional start date for data collection
            end_date: Optional end date for data collection
            
        Returns:
            ETLConfig: Configuration initialized from environment
        """
        import os
        from dotenv import load_dotenv
        
        load_dotenv(env_file)
        
        config = cls(start_date=start_date, end_date=end_date)
        config.api.token = os.getenv('GITHUB_API_TOKEN', '')
        config.bigquery.project_id = os.getenv('BIGQUERY_PROJECT_ID', '')
        config.bigquery.credentials_path = Path(os.getenv('GOOGLE_APPLICATION_CREDENTIALS', '')) if os.getenv('GOOGLE_APPLICATION_CREDENTIALS') else None
        config.bigquery.max_bytes_billed = int(os.getenv('BIGQUERY_MAX_BYTES', 20_000_000_000))
        config.database.url = os.getenv('DATABASE_URL', 'sqlite:///github_data.db')
        
        return config
    
    def save_state(self, state: ProcessingState) -> None:
        """Save current processing state to file."""
        import json
        state_dict = {
            "last_processed_date": state.last_processed_date.isoformat(),
            "last_processed_hour": state.last_processed_hour,
            "processed_repo_ids": list(state.processed_repo_ids),
            "failed_repo_ids": list(state.failed_repo_ids),
            "event_counts": state.event_counts
        }
        with open(self.state_file, 'w') as f:
            json.dump(state_dict, f)
    
    def load_state(self) -> Optional[ProcessingState]:
        """Load previous processing state if exists."""
        if not self.state_file.exists():
            return None
            
        import json
        with open(self.state_file, 'r') as f:
            state_dict = json.load(f)
            
        return ProcessingState(
            last_processed_date=datetime.fromisoformat(state_dict["last_processed_date"]),
            last_processed_hour=state_dict["last_processed_hour"],
            processed_repo_ids=set(state_dict["processed_repo_ids"]),
            failed_repo_ids=set(state_dict["failed_repo_ids"]),
            event_counts=state_dict["event_counts"]
        )
