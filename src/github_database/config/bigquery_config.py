"""BigQuery configuration for GitHub Archive data."""

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from google.oauth2 import service_account
from google.cloud import bigquery

@dataclass
class BigQueryConfig:
    """BigQuery configuration for GitHub Archive data."""
    project_id: str
    dataset_id: str = "githubarchive"
    table_id: str = "day"  # GitHub Archive uses day_YYYYMMDD tables
    credentials_path: Optional[Path] = None
    max_bytes_billed: int = 1_000_000_000  # 1GB
    batch_size: int = 1000
    max_query_days: int = 30  # Maximum days to query in a single batch
    
    @property
    def full_table_id(self) -> str:
        """Get fully qualified table ID including project and dataset."""
        return f"{self.project_id}.{self.dataset_id}.{self.table_id}"
        
    def get_credentials(self) -> Optional[service_account.Credentials]:
        """Get BigQuery credentials."""
        if self.credentials_path and self.credentials_path.exists():
            return service_account.Credentials.from_service_account_file(
                str(self.credentials_path),
                scopes=["https://www.googleapis.com/auth/bigquery"]
            )
        return None
        
    @classmethod
    def from_env(cls) -> 'BigQueryConfig':
        """Create configuration from environment variables."""
        return cls(
            project_id=os.getenv('BIGQUERY_PROJECT_ID', 'githubarchive'),  # Default to public dataset
            dataset_id=os.getenv('BIGQUERY_DATASET_ID', 'githubarchive'),
            table_id=os.getenv('BIGQUERY_TABLE_ID', 'day'),
            credentials_path=Path(os.getenv('GOOGLE_APPLICATION_CREDENTIALS', '')) if os.getenv('GOOGLE_APPLICATION_CREDENTIALS') else None,
            max_bytes_billed=int(os.getenv('BIGQUERY_MAX_BYTES', '1000000000')),
            batch_size=int(os.getenv('BIGQUERY_BATCH_SIZE', '1000')),
            max_query_days=int(os.getenv('BIGQUERY_MAX_QUERY_DAYS', '30'))
        )
