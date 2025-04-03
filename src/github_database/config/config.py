"""
Central configuration for GitHub data ETL.

This module provides unified configuration classes for all components of the
GitHub data ETL system, including:
- GitHub API access
- Database connections
- Quality filters for repositories
- Performance optimizations like token pool and caching
"""

import os
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, List, Dict, Any, Union

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

logger = logging.getLogger(__name__)

@dataclass
class GitHubConfig:
    """
    GitHub API configuration.
    
    Contains all settings for accessing the GitHub API, including
    token management, rate limit handling, and request optimizations.
    """
    
    access_token: str
    api_url: str = "https://api.github.com"
    rate_limit_delay: float = 0.1  # Delay between API calls
    parallel_requests: int = 2  # Number of parallel requests
    retry_count: int = 3  # Number of retries for failed requests
    retry_delay: float = 1.0  # Delay between retry attempts in seconds
    per_page: int = 100  # Maximum number of results per page
    rate_limit_pause: int = 60  # Pause time (in seconds) when rate limit is exceeded
    
    # Token pool settings
    use_token_pool: bool = False
    additional_tokens: List[str] = field(default_factory=list)
    
    def __post_init__(self):
        """Validate configuration after initialization."""
        if not self.access_token:
            raise ValueError("GitHub access token is required")
    
    @classmethod
    def from_env(cls) -> 'GitHubConfig':
        """Create configuration from environment variables."""
        # Main token
        token = os.getenv('GITHUB_TOKEN')
        if not token:
            token = os.getenv('GITHUB_API_TOKEN')  # Fallback
            
        if not token:
            raise ValueError("GitHub token not found in environment variables. "
                           "Please set GITHUB_TOKEN or GITHUB_API_TOKEN.")
        
        # Additional tokens for token pool
        additional_tokens = []
        token_list_str = os.getenv('GITHUB_ADDITIONAL_TOKENS', '')
        if token_list_str:
            # Tokens can be provided as comma-separated list
            additional_tokens = [t.strip() for t in token_list_str.split(',') if t.strip()]
        
        use_token_pool = bool(additional_tokens) or os.getenv('GITHUB_USE_TOKEN_POOL', '').lower() == 'true'
        
        return cls(
            access_token=token,
            api_url=os.getenv('GITHUB_API_URL', 'https://api.github.com'),
            rate_limit_delay=float(os.getenv('GITHUB_RATE_LIMIT_DELAY', '0.1')),
            parallel_requests=int(os.getenv('GITHUB_PARALLEL_REQUESTS', '2')),
            retry_count=int(os.getenv('GITHUB_RETRY_COUNT', '3')),
            retry_delay=float(os.getenv('GITHUB_RETRY_DELAY', '1.0')),
            per_page=int(os.getenv('GITHUB_PER_PAGE', '100')),
            rate_limit_pause=int(os.getenv('GITHUB_RATE_LIMIT_PAUSE', '60')),
            use_token_pool=use_token_pool,
            additional_tokens=additional_tokens
        )


@dataclass
class QualityConfig:
    """
    Quality thresholds for repositories.
    
    Defines minimum requirements for repositories to be included in the dataset.
    """
    
    min_stars: int = 50  # Minimum number of stars
    min_forks: int = 10  # Minimum number of forks
    min_commits_last_year: int = 100  # Minimum commit activity in the last year
    min_contributors: int = 3  # Minimum number of contributors
    
    @classmethod
    def from_env(cls) -> 'QualityConfig':
        """Create quality configuration from environment variables."""
        return cls(
            min_stars=int(os.getenv('QUALITY_MIN_STARS', '50')),
            min_forks=int(os.getenv('QUALITY_MIN_FORKS', '10')),
            min_commits_last_year=int(os.getenv('QUALITY_MIN_COMMITS', '100')),
            min_contributors=int(os.getenv('QUALITY_MIN_CONTRIBUTORS', '3'))
        )


@dataclass
class CacheConfig:
    """
    Cache configuration for API requests and data.
    
    Defines settings for various caching levels to optimize
    API requests and database interactions.
    """
    
    enabled: bool = True
    cache_dir: Path = field(default_factory=lambda: Path("/tmp/github-etl/cache"))
    max_age: int = 86400  # Maximum age of cache entries in seconds (1 day)
    repository_cache_size: int = 10000  # Number of repositories to cache
    user_cache_size: int = 5000  # Number of users to cache
    organization_cache_size: int = 1000  # Number of organizations to cache
    search_cache_size: int = 500  # Number of search queries to cache
    
    def __post_init__(self):
        """Create cache directory if it does not exist."""
        if self.enabled and not self.cache_dir.exists():
            self.cache_dir.mkdir(parents=True, exist_ok=True)
    
    @classmethod
    def from_env(cls) -> 'CacheConfig':
        """Create cache configuration from environment variables."""
        cache_dir_str = os.getenv('CACHE_DIR', '/tmp/github-etl/cache')
        
        return cls(
            enabled=os.getenv('CACHE_ENABLED', 'true').lower() in ('true', '1', 'yes'),
            cache_dir=Path(cache_dir_str),
            max_age=int(os.getenv('CACHE_MAX_AGE', '86400')),
            repository_cache_size=int(os.getenv('CACHE_REPO_SIZE', '10000')),
            user_cache_size=int(os.getenv('CACHE_USER_SIZE', '5000')),
            organization_cache_size=int(os.getenv('CACHE_ORG_SIZE', '1000')),
            search_cache_size=int(os.getenv('CACHE_SEARCH_SIZE', '500'))
        )


@dataclass
class ETLConfig:
    """
    Central ETL configuration.
    
    Integrates all configuration components for the ETL process, including
    database, GitHub API, and performance optimizations.
    """
    
    database_url: str = field(default="sqlite:///github_data.db")
    cache_dir: str = field(default="/tmp/github-etl")
    batch_size: int = field(default=1000)
    max_workers: int = field(default=10)  # Maximum number of parallel workers
    limit: int = field(default=0)  # 0 means no limit
    geocoding_enabled: bool = field(default=True)
    
    # Component-specific configurations
    quality: QualityConfig = field(default_factory=QualityConfig)
    github: GitHubConfig = field(default_factory=lambda: GitHubConfig(access_token="dummy_token"))
    cache: CacheConfig = field(default_factory=CacheConfig)
    
    # Internal fields, not set through constructor
    _session: Optional[Session] = field(default=None, repr=False, init=False)
    _engine: Any = field(default=None, repr=False, init=False)
    _session_maker: Any = field(default=None, repr=False, init=False)
    
    def __post_init__(self):
        """Validate the configuration after initialization."""
        if "sqlite:///" in self.database_url:
            db_path = self.database_url.replace("sqlite:///", "")
            logger.info(f"SQLite database will be used: {db_path}")
        
        # Create cache directory if it does not exist
        if not os.path.exists(self.cache_dir):
            try:
                os.makedirs(self.cache_dir, exist_ok=True)
                logger.info(f"Cache directory created: {self.cache_dir}")
            except Exception as e:
                logger.warning(f"Failed to create cache directory: {e}")
    
    def get_engine(self):
        """Create and return SQLAlchemy engine."""
        if not self._engine:
            self._engine = create_engine(self.database_url)
        return self._engine
    
    def get_session_maker(self):
        """Create and return session factory."""
        if not self._session_maker:
            engine = self.get_engine()
            self._session_maker = sessionmaker(bind=engine)
        return self._session_maker
    
    def get_session(self) -> Session:
        """Create database session."""
        if not self._session:
            session_maker = self.get_session_maker()
            self._session = session_maker()
        return self._session
        
    def close_session(self) -> None:
        """Close the current session."""
        if self._session:
            self._session.close()
            self._session = None
    
    @classmethod
    def from_env(cls) -> 'ETLConfig':
        """Create configuration from environment variables."""
        database_url = os.getenv('DATABASE_URL', 'sqlite:///github_data.db')
        cache_dir = os.getenv('CACHE_DIR', '/tmp/github-etl')
        
        # Create components from environment variables
        github_config = GitHubConfig.from_env()
        quality_config = QualityConfig.from_env()
        cache_config = CacheConfig.from_env()
        
        return cls(
            database_url=database_url,
            cache_dir=cache_dir,
            batch_size=int(os.getenv('BATCH_SIZE', '1000')),
            max_workers=int(os.getenv('MAX_WORKERS', '10')),
            limit=int(os.getenv('LIMIT', '0')),
            geocoding_enabled=os.getenv('GEOCODING_ENABLED', 'true').lower() in ('true', '1', 'yes'),
            quality=quality_config,
            github=github_config,
            cache=cache_config
        )


def load_config() -> ETLConfig:
    """
    Central function for loading the configuration.
    
    Loads the configuration from environment variables and returns it.
    This is the recommended method for creating a configuration.
    
    Returns:
        ETLConfig: Fully initialized ETL configuration
    """
    try:
        config = ETLConfig.from_env()
        logger.info("Configuration successfully loaded from environment variables")
        return config
    except Exception as e:
        logger.error(f"Error loading configuration: {e}")
        raise
