"""Repository quality thresholds configuration."""

from dataclasses import dataclass
from typing import Optional, Set

@dataclass
class QualityThresholds:
    """Repository quality thresholds."""
    min_stars: int = 50
    min_forks: int = 10
    min_commits_last_year: int = 100
    languages: Optional[Set[str]] = None
