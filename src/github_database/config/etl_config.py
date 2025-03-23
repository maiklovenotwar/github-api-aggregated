"""ETL configuration module."""

import os
from dataclasses import dataclass, field
from typing import Optional
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from .github_config import GitHubConfig
from .bigquery_config import BigQueryConfig

@dataclass
class QualityConfig:
    """Quality thresholds for repositories."""
    
    min_stars: int = 50
    min_forks: int = 10
    min_commits_last_year: int = 100

@dataclass
class ETLConfig:
    """ETL configuration."""
    
    cache_dir: str = field(default="/tmp/github-etl")
    database_url: str = field(default="sqlite:///github.db")
    batch_size: int = field(default=1000)
    quality: QualityConfig = field(default_factory=QualityConfig)
    github: GitHubConfig = field(default_factory=lambda: GitHubConfig(access_token="test_token"))
    bigquery: BigQueryConfig = field(default_factory=lambda: BigQueryConfig(
        project_id="test-project",
        dataset_id="github_archive",
        credentials_path=None
    ))
    _session: Optional[Session] = field(default=None, init=False)
    _engine: Optional[object] = field(default=None, init=False)
    _session_maker: Optional[object] = field(default=None, init=False)
    
    def get_session(self) -> Session:
        """Create database session."""
        if not self._engine:
            self._engine = create_engine(self.database_url)
            self._session_maker = sessionmaker(bind=self._engine)
        if not self._session:
            self._session = self._session_maker()
        return self._session
        
    def close_session(self) -> None:
        """Close the current session."""
        if self._session:
            self._session.close()
            self._session = None
    
    @classmethod
    def from_env(cls) -> 'ETLConfig':
        """Create ETL config from environment variables."""
        return cls(
            cache_dir=os.getenv('ETL_CACHE_DIR', '/tmp/github-etl'),
            database_url=os.getenv('DATABASE_URL', 'sqlite:///github.db'),
            batch_size=int(os.getenv('ETL_BATCH_SIZE', '1000')),
            quality=QualityConfig(
                min_stars=int(os.getenv('MIN_STARS', '50')),
                min_forks=int(os.getenv('MIN_FORKS', '10')),
                min_commits_last_year=int(os.getenv('MIN_COMMITS_LAST_YEAR', '100'))
            )
        )
