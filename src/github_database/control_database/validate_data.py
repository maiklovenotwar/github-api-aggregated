"""Data validation for GitHub events and entities."""

import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
from src.github_database.database.database import get_session, Repository

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataValidator:
    """Validates GitHub event data before processing."""

    EVENT_TYPES = {
        'PushEvent', 'PullRequestEvent', 'IssuesEvent',
        'ForkEvent', 'WatchEvent', 'StarEvent'
    }

    def __init__(self):
        """Initialize the validator with validation rules."""
        self.validation_rules = {
            'PushEvent': self._validate_push_event,
            'PullRequestEvent': self._validate_pull_request_event,
            'IssuesEvent': self._validate_issue_event,
            'ForkEvent': self._validate_fork_event,
            'WatchEvent': self._validate_watch_event,
            'StarEvent': self._validate_star_event
        }

    def validate_events(self, events: List[Dict]) -> List[Dict]:
        """
        Validate a list of events.
        
        Args:
            events: List of events to validate
            
        Returns:
            List of valid events
        """
        valid_events = []
        for event in events:
            try:
                if self._is_valid_event(event):
                    valid_events.append(event)
                else:
                    logger.warning(f"Invalid event structure: {event.get('id', 'unknown')}")
            except Exception as e:
                logger.error(f"Event validation failed: {e}")
        return valid_events

    def _is_valid_event(self, event: Dict) -> bool:
        """
        Check if an event is valid.
        
        Args:
            event: Event to validate
            
        Returns:
            bool: True if event is valid
        """
        # Check basic event structure
        if not self._validate_basic_structure(event):
            return False

        # Get event type and validate
        event_type = event.get('type')
        if event_type not in self.EVENT_TYPES:
            logger.warning(f"Unknown event type: {event_type}")
            return False

        # Apply event-specific validation
        validator = self.validation_rules.get(event_type)
        if validator and not validator(event):
            return False

        return True

    def _validate_basic_structure(self, event: Dict) -> bool:
        """Validate basic event structure."""
        required_fields = {'id', 'type', 'actor', 'repo', 'created_at'}
        
        # Check required fields exist
        if not all(field in event for field in required_fields):
            return False
            
        # Validate actor structure
        if not self._validate_actor(event.get('actor', {})):
            return False
            
        # Validate repo structure
        if not self._validate_repo(event.get('repo', {})):
            return False
            
        # Validate timestamp
        try:
            datetime.strptime(event['created_at'], '%Y-%m-%dT%H:%M:%SZ')
        except (ValueError, TypeError):
            return False
            
        return True

    def _validate_actor(self, actor: Dict) -> bool:
        """Validate actor information."""
        required_fields = {'id', 'login'}
        return all(field in actor for field in required_fields)

    def _validate_repo(self, repo: Dict) -> bool:
        """Validate repository information."""
        required_fields = {'id', 'name'}
        return all(field in repo for field in required_fields)

    def _validate_push_event(self, event: Dict) -> bool:
        """Validate push event specific fields."""
        payload = event.get('payload', {})
        if not payload:
            return False
            
        # Check for commits array
        commits = payload.get('commits', [])
        if not isinstance(commits, list):
            return False
            
        # Validate each commit
        for commit in commits:
            if not self._validate_commit(commit):
                return False
                
        return True

    def _validate_pull_request_event(self, event: Dict) -> bool:
        """Validate pull request event specific fields."""
        payload = event.get('payload', {})
        if not payload:
            return False
            
        pull_request = payload.get('pull_request', {})
        required_fields = {'id', 'number', 'title', 'state'}
        return all(field in pull_request for field in required_fields)

    def _validate_issue_event(self, event: Dict) -> bool:
        """Validate issue event specific fields."""
        payload = event.get('payload', {})
        if not payload:
            return False
            
        issue = payload.get('issue', {})
        required_fields = {'id', 'number', 'title', 'state'}
        return all(field in issue for field in required_fields)

    def _validate_fork_event(self, event: Dict) -> bool:
        """Validate fork event specific fields."""
        payload = event.get('payload', {})
        if not payload:
            return False
            
        forkee = payload.get('forkee', {})
        required_fields = {'id', 'full_name'}
        return all(field in forkee for field in required_fields)

    def _validate_watch_event(self, event: Dict) -> bool:
        """Validate watch event specific fields."""
        payload = event.get('payload', {})
        return payload and 'action' in payload

    def _validate_star_event(self, event: Dict) -> bool:
        """Validate star event specific fields."""
        payload = event.get('payload', {})
        return payload and 'action' in payload

    def _validate_commit(self, commit: Dict) -> bool:
        """Validate commit structure."""
        required_fields = {'sha', 'message', 'author'}
        if not all(field in commit for field in required_fields):
            return False
            
        # Validate author information
        author = commit.get('author', {})
        author_fields = {'name', 'email'}
        return all(field in author for field in author_fields)

def validate_repository(repository: Repository) -> bool:
    """
    Validate a single repository.
    
    Args:
        repository: Repository object to validate
        
    Returns:
        bool: True if repository is valid
    """
    # Check required fields
    if not repository.id or not repository.name or not repository.full_name:
        logger.warning(f"Repository {repository.id} missing required fields")
        return False
        
    # Check numeric fields are non-negative
    if repository.stars_count < 0 or repository.forks_count < 0:
        logger.warning(f"Repository {repository.id} has negative counts")
        return False
        
    # Check timestamps
    if not repository.created_at or not repository.updated_at:
        logger.warning(f"Repository {repository.id} missing timestamps")
        return False
        
    if repository.updated_at < repository.created_at:
        logger.warning(f"Repository {repository.id} has invalid timestamps")
        return False
        
    return True

def validate_repositories() -> bool:
    """
    Validate all repositories in the database.
    
    Returns:
        bool: True if all repositories are valid
    """
    session = get_session()
    try:
        repositories = session.query(Repository).all()
        for repo in repositories:
            if not validate_repository(repo):
                return False
        return True
    finally:
        session.close()

def main():
    validate_repositories()

if __name__ == '__main__':
    main()