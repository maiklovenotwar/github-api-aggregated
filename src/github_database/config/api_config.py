"""GitHub API configuration."""

from dataclasses import dataclass
from typing import Optional, Set

@dataclass
class APIConfig:
    """GitHub API configuration."""
    token: str
    requests_per_hour: int = 5000
    min_remaining_rate: int = 100
    retry_wait_time: int = 60
