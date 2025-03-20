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
    token: str
    requests_per_hour: int = 5000
    min_remaining_rate: int = 100
    retry_wait_time: int = 60

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
    last_processed_date: datetime
    last_processed_hour: int = 0
    processed_repo_ids: Set[int] = field(default_factory=set)
    failed_repo_ids: Set[int] = field(default_factory=set)
    event_counts: Dict[str, int] = field(default_factory=dict)

@dataclass
class ETLConfig:
    """Master configuration for ETL process."""
    quality: QualityThresholds
    api: APIConfig
    archive: ArchiveConfig
    database: DatabaseConfig
    start_date: datetime
    end_date: datetime
    state_file: Path = Path("etl_state.json")
    
    @classmethod
    def from_env(cls, env_file: Path = Path(".env")):
        """Create configuration from environment variables."""
        # Implementation for loading from .env file
        pass
    
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
