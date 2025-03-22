"""Processing state configuration."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Set, Dict

@dataclass
class ProcessingState:
    """Track processing progress for resume capability."""
    last_processed_date: datetime
    last_processed_hour: int = 0
    processed_repo_ids: Set[int] = field(default_factory=set)
    failed_repo_ids: Set[int] = field(default_factory=set)
    event_counts: Dict[str, int] = field(default_factory=dict)
