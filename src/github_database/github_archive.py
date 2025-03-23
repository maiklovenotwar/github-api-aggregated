"""GitHub Archive event types and utilities."""

from enum import Enum
from typing import Optional

class EventType(str, Enum):
    """GitHub event types."""
    PUSH = 'PushEvent'
    PULL_REQUEST = 'PullRequestEvent'
    ISSUES = 'IssuesEvent'
    FORK = 'ForkEvent'
    WATCH = 'WatchEvent'
    CREATE = 'CreateEvent'
    DELETE = 'DeleteEvent'
    GOLLUM = 'GollumEvent'  # Wiki events
    MEMBER = 'MemberEvent'
    PUBLIC = 'PublicEvent'
    ORGANIZATION = 'OrganizationEvent'
    
    @classmethod
    def from_str(cls, value: str) -> Optional['EventType']:
        """Convert string to EventType."""
        try:
            return cls(value)
        except ValueError:
            return None
