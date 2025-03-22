"""GitHub Archive configuration."""

from dataclasses import dataclass
from pathlib import Path

@dataclass
class ArchiveConfig:
    """GitHub Archive configuration."""
    cache_dir: Path = Path("cache/github_archive")
    batch_size: int = 1000
    max_daily_events: int = 1000000
    parallel_downloads: int = 5
