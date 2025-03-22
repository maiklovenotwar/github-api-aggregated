"""Repository mapper for converting GitHub Archive events to database models."""

import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from pathlib import Path
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from dateutil import parser

from ..config import ETLConfig
from ..database.database import (
    Base,
    Repository,
    User,
    Event,
    Commit,
    PullRequest,
    Issue,
    Fork,
    Star,
    Watch
)

logger = logging.getLogger(__name__)

@dataclass
class EventValidationError(Exception):
    """Raised when event validation fails."""
    event_type: str
    event_id: str
    reason: str
    
    def __str__(self):
        return self.reason

@dataclass
class EventValidator:
    """Validate GitHub Archive events."""
    
    def validate_event(self, event_dict: Dict) -> None:
        """
        Validate event structure and required fields.
        
        Args:
            event_dict: Raw event dictionary from GitHub Archive
            
        Raises:
            EventValidationError: If validation fails
        """
        # Check required fields
        required_fields = ['id', 'type', 'actor', 'repo', 'created_at']
        missing_fields = [field for field in required_fields if field not in event_dict]
        
        if missing_fields:
            raise EventValidationError(
                event_type=event_dict.get('type', 'unknown'),
                event_id=event_dict.get('id', 'unknown'),
                reason=f"Missing required fields: {', '.join(missing_fields)}"
            )
            
        # Validate payload schema
        event_type = event_dict['type']
        payload_schemas = {
            'PushEvent': {'ref', 'commits'},
            'PullRequestEvent': {'action', 'pull_request'},
            'IssuesEvent': {'action', 'issue'},
            'ForkEvent': {'forkee'},
            'WatchEvent': {'action'},
            'StarEvent': {'action'}
        }
        
        if event_type in payload_schemas:
            payload = event_dict.get('payload', {})
            required_payload_fields = payload_schemas[event_type]
            missing_payload_fields = [field for field in required_payload_fields if field not in payload]
            
            if missing_payload_fields:
                raise EventValidationError(
                    event_type=event_type,
                    event_id=event_dict['id'],
                    reason=f"Missing payload fields: {', '.join(missing_payload_fields)}"
                )

class RepositoryMapper:
    """Map GitHub Archive events to database models."""
    
    def __init__(self, session: Session, config: ETLConfig, enricher):
        """Initialize mapper."""
        self.session = session
        self.config = config
        self.enricher = enricher
        self._repo_cache = {}
        self._user_cache = {}
        self.validator = EventValidator()
        
    def _extract_repository(self, repo_dict: Dict) -> Repository:
        """
        Extract repository information and get or create Repository object.
        
        Args:
            repo_dict: Repository dictionary from event
            
        Returns:
            Repository: SQLAlchemy Repository object
        """
        # Enrich repository data
        repo_dict = self.enricher.enrich_repository(repo_dict)
        repo_id = int(repo_dict['id'])
        
        if repo_id not in self._repo_cache:
            try:
                repo = self.session.query(Repository).get(repo_id)
                if not repo:
                    # Parse owner/name from full_name
                    name_parts = repo_dict['name'].split('/')
                    owner = name_parts[0] if len(name_parts) > 1 else None
                    name = name_parts[-1]
                    
                    repo = Repository(
                        id=repo_id,
                        name=name,
                        full_name=repo_dict['name'],
                        description=repo_dict.get('description'),
                        language=repo_dict.get('language'),
                        stars_count=repo_dict.get('stars', 0),
                        forks_count=repo_dict.get('forks', 0),
                        created_at=datetime.now()
                    )
                    self.session.add(repo)
                self._repo_cache[repo_id] = repo
            except Exception as e:
                logger.error(f"Error getting/creating repository {repo_id}: {e}")
                raise
                
        return self._repo_cache[repo_id]
        
    def _extract_user(self, user_dict: Dict) -> User:
        """
        Extract user information and get or create User object.
        
        Args:
            user_dict: User dictionary from event
            
        Returns:
            User: SQLAlchemy User object
        """
        # Enrich user data
        user_dict = self.enricher.enrich_user(user_dict)
        user_id = int(user_dict['id'])
        
        if user_id not in self._user_cache:
            try:
                user = self.session.query(User).get(user_id)
                if not user:
                    user = User(
                        id=user_id,
                        login=user_dict.get('login'),
                        name=user_dict.get('name'),
                        email=user_dict.get('email'),
                        created_at=datetime.now()
                    )
                    self.session.add(user)
                self._user_cache[user_id] = user
            except Exception as e:
                logger.error(f"Error getting/creating user {user_id}: {e}")
                raise
                
        return self._user_cache[user_id]
        
    def _extract_timestamp(self, event: Dict, field: str) -> Optional[datetime]:
        """Extract timestamp from event dictionary."""
        if not event or field not in event:
            return None
            
        timestamp_str = event[field]
        if not timestamp_str:
            return None
            
        try:
            return datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc)
        except Exception as e:
            logger.warning(f"Error parsing timestamp {timestamp_str}: {e}")
            return None

    def _create_event(self, event: Dict) -> Event:
        """Create Event object from event dictionary."""
        repo = self._extract_repository(event['repo'])
        actor = self._extract_user(event['actor'])
        
        event_obj = Event(
            event_id=event['id'],
            type=event['type'],
            actor_id=actor.id,
            repo_id=repo.id,
            payload=event['payload'],
            created_at=self._extract_timestamp(event, 'created_at')
        )
        
        return event_obj

    def map_pushevent(self, event_dict: Dict) -> Tuple[Event, List[Commit]]:
        """Map PushEvent to Event and Commit objects."""
        try:
            event_obj = self._create_event(event_dict)
            commits = []
            
            # Process each commit
            for commit_data in event_dict['payload'].get('commits', []):
                try:
                    commit = Commit(
                        sha=commit_data['sha'],
                        message=commit_data['message'],
                        author_name=commit_data['author']['name'],
                        author_email=commit_data['author']['email'],
                        timestamp=self._extract_timestamp(commit_data, 'timestamp'),
                        repository_id=event_obj.repo_id,
                        user_id=event_obj.actor_id
                    )
                    commits.append(commit)
                except Exception as e:
                    logger.warning(f"Error processing commit {commit_data.get('sha')}: {str(e)}")
                    
            return event_obj, commits
        except Exception as e:
            logger.error(f"Error mapping PushEvent: {str(e)}")
            raise

    def map_pullrequestevent(self, event_dict: Dict) -> Tuple[Event, PullRequest]:
        """Map PullRequestEvent to Event and PullRequest objects."""
        try:
            event_obj = self._create_event(event_dict)
            pr_data = event_dict['payload']['pull_request']
            
            # Enrich pull request data
            pr_data = self.enricher.enrich_pull_request(pr_data, event_dict['repo']['name'])
            
            # Create pull request object
            pull_request = PullRequest(
                id=pr_data['id'],
                number=pr_data['number'],
                title=pr_data['title'],
                body=pr_data['body'],
                state=pr_data['state'],
                created_at=self._extract_timestamp(pr_data, 'created_at'),
                updated_at=self._extract_timestamp(pr_data, 'updated_at'),
                head_ref=pr_data['head']['ref'],
                base_ref=pr_data['base']['ref'],
                additions=pr_data.get('additions', 0),
                deletions=pr_data.get('deletions', 0),
                changed_files=pr_data.get('changed_files', 0),
                repository_id=event_obj.repo_id,
                user_id=event_obj.actor_id
            )
            
            return event_obj, pull_request
        except Exception as e:
            logger.error(f"Error mapping PullRequestEvent: {str(e)}")
            raise

    def map_issuesevent(self, event_dict: Dict) -> Tuple[Event, Issue]:
        """Map IssuesEvent to Event and Issue objects."""
        try:
            event_obj = self._create_event(event_dict)
            issue_data = event_dict['payload']['issue']
            
            # Enrich issue data
            issue_data = self.enricher.enrich_issue(issue_data, event_dict['repo']['name'])
            
            # Create issue object
            issue = Issue(
                id=issue_data['id'],
                number=issue_data['number'],
                title=issue_data['title'],
                body=issue_data['body'],
                state=issue_data['state'],
                created_at=self._extract_timestamp(issue_data, 'created_at'),
                updated_at=self._extract_timestamp(issue_data, 'updated_at'),
                repository_id=event_obj.repo_id,
                user_id=event_obj.actor_id
            )
            
            return event_obj, issue
        except Exception as e:
            logger.error(f"Error mapping IssuesEvent: {str(e)}")
            raise

    def map_forkevent(self, event_dict: Dict) -> Tuple[Event, Fork]:
        """Map ForkEvent to database models."""
        try:
            # Extract common fields
            repo = self._extract_repository(event_dict['repo'])
            user = self._extract_user(event_dict['actor'])
            created_at = self._extract_timestamp(event_dict, 'created_at')
            
            # Extract fork details
            forkee = event_dict['payload']['forkee']
            forked_repo = self._extract_repository(forkee)
            
            # Create event
            event = Event(
                event_id=event_dict['id'],
                type=event_dict['type'],
                actor_id=user.id,
                repo_id=repo.id,
                payload=event_dict['payload'],
                created_at=created_at
            )
            
            # Create fork
            fork = Fork(
                repository_id=forked_repo.id,  # ID of the forked repository
                parent_id=repo.id,  # ID of the source repository
                user_id=user.id,
                forked_at=created_at
            )
            
            return event, fork
        except Exception as e:
            logger.error(f"Error mapping ForkEvent: {e}")
            raise

    def map_starevent(self, event_dict: Dict) -> Tuple[Event, Star]:
        """Map StarEvent to database models."""
        try:
            # Extract common fields
            repo = self._extract_repository(event_dict['repo'])
            user = self._extract_user(event_dict['actor'])
            created_at = self._extract_timestamp(event_dict, 'created_at')
            
            # Create event
            event = Event(
                event_id=event_dict['id'],
                type=event_dict['type'],
                actor_id=user.id,
                repo_id=repo.id,
                payload=event_dict['payload'],
                created_at=created_at
            )
            
            # Create star
            star = Star(
                repository_id=repo.id,
                user_id=user.id,
                starred_at=created_at
            )
            
            return event, star
        except Exception as e:
            logger.error(f"Error mapping StarEvent: {e}")
            raise

    def map_watchevent(self, event_dict: Dict) -> Tuple[Event, Watch]:
        """Map WatchEvent to database models."""
        try:
            # Extract common fields
            repo = self._extract_repository(event_dict['repo'])
            user = self._extract_user(event_dict['actor'])
            created_at = self._extract_timestamp(event_dict, 'created_at')
            
            # Create event
            event = Event(
                event_id=event_dict['id'],
                type=event_dict['type'],
                actor_id=user.id,
                repo_id=repo.id,
                payload=event_dict['payload'],
                created_at=created_at
            )
            
            # Create watch
            watch = Watch(
                repository_id=repo.id,
                user_id=user.id,
                watched_at=created_at
            )
            
            return event, watch
        except Exception as e:
            logger.error(f"Error mapping WatchEvent: {e}")
            raise
