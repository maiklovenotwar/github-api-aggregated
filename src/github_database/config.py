"""Configuration for GitHub data collection and aggregation."""

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

@dataclass
class GitHubConfig:
    """GitHub API configuration."""
    access_token: str
    base_url: str = "https://api.github.com"
    per_page: int = 100
    rate_limit_pause: int = 60  # Sekunden

@dataclass
class BigQueryConfig:
    """BigQuery configuration."""
    project_id: str
    dataset_id: str
    table_id: str
    credentials_path: Path
    max_bytes_billed: int = 1_000_000_000  # 1 GB
    
    @property
    def max_bytes(self) -> int:
        """Get maximum bytes billed."""
        return self.max_bytes_billed

@dataclass
class ETLConfig:
    """ETL configuration."""
    database_url: str
    github: GitHubConfig
    bigquery: BigQueryConfig
    
    # Qualitätsfilter für Repositories
    min_stars: int = 10  # Geändert von 50 auf 10
    min_forks: int = 0   # Geändert von 10 auf 0
    min_commits_last_year: int = 0  # Geändert von 100 auf 0
    
    # Batch-Größe für Verarbeitung
    batch_size: int = 1000
    
    # Limit für die Anzahl der zu verarbeitenden Repositories
    limit: int = 50
