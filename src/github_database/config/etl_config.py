"""ETL configuration."""

import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

from .api_config import APIConfig
from .archive_config import ArchiveConfig
from .bigquery_config import BigQueryConfig
from .database_config import DatabaseConfig
from .processing_state import ProcessingState
from .quality_config import QualityThresholds

@dataclass
class ETLConfig:
    """Master configuration for ETL process."""
    start_date: datetime
    end_date: datetime
    quality: QualityThresholds = field(default_factory=QualityThresholds)
    api: APIConfig = field(default_factory=lambda: APIConfig(token=""))
    bigquery: BigQueryConfig = field(default_factory=lambda: BigQueryConfig(project_id=""))
    archive: ArchiveConfig = field(default_factory=ArchiveConfig)
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    state: ProcessingState = field(default_factory=lambda: ProcessingState(last_processed_date=datetime.now()))
    state_file: Path = Path("etl_state.json")

    # General settings
    batch_size: int = 1000
    max_parallel_tasks: int = 5
    memory_limit_mb: int = 1024
    cache_dir: Path = Path("cache")
    error_file: Path = Path("errors.csv")

    @classmethod
    def from_env(cls, days_back: int = 7) -> 'ETLConfig':
        """Create configuration from environment variables."""
        load_dotenv()

        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)

        # Create API config
        api_config = APIConfig(
            token=os.getenv('GITHUB_API_TOKEN', ''),
            requests_per_hour=int(os.getenv('GITHUB_API_RATE_LIMIT', '5000')),
            min_remaining_rate=int(os.getenv('GITHUB_API_MIN_REMAINING', '100')),
            retry_wait_time=int(os.getenv('GITHUB_API_RETRY_WAIT', '60'))
        )

        # Create BigQuery config
        bigquery_config = BigQueryConfig(
            project_id=os.getenv('BIGQUERY_PROJECT_ID', ''),
            dataset_id=os.getenv('BIGQUERY_DATASET_ID', 'githubarchive'),
            credentials_path=Path(os.getenv('GOOGLE_APPLICATION_CREDENTIALS', '')) if os.getenv('GOOGLE_APPLICATION_CREDENTIALS') else None,
            max_bytes_billed=int(os.getenv('BIGQUERY_MAX_BYTES', '1000000000'))
        )

        # Create database config
        database_config = DatabaseConfig(
            url=os.getenv('DATABASE_URL', 'sqlite:///github_data.db'),
            batch_size=int(os.getenv('DATABASE_BATCH_SIZE', '500'))
        )

        # Create quality thresholds
        quality_config = QualityThresholds(
            min_stars=int(os.getenv('MIN_STARS', '50')),
            min_forks=int(os.getenv('MIN_FORKS', '10')),
            min_commits_last_year=int(os.getenv('MIN_COMMITS_LAST_YEAR', '100'))
        )

        return cls(
            start_date=start_date,
            end_date=end_date,
            api=api_config,
            bigquery=bigquery_config,
            database=database_config,
            quality=quality_config,
            batch_size=int(os.getenv('BATCH_SIZE', '1000')),
            max_parallel_tasks=int(os.getenv('MAX_PARALLEL_TASKS', '5')),
            memory_limit_mb=int(os.getenv('MEMORY_LIMIT_MB', '1024')),
            cache_dir=Path(os.getenv('CACHE_DIR', 'cache')),
            error_file=Path(os.getenv('ERROR_FILE', 'errors.csv'))
        )
