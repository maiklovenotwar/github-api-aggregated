"""Repository mapper for converting GitHub Archive events to database models."""

import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Type, Any, Union, Tuple
from dataclasses import dataclass
from pathlib import Path
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from dateutil import parser
import uuid

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
    Watch,
    PushEventData,
    PullRequestEventData,
    IssueEventData,
    ForkEventData,
    WatchEventData,
    CommitData
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
        required_fields = ['id', 'type', 'actor_id', 'actor_login', 'created_at']
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
                logger.warning(f"Missing payload fields for {event_type}: {', '.join(missing_payload_fields)}")
                # Don't raise error for missing payload fields as they might be optional

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
        
    def _extract_repository(self, repo_id: str, repo_name: str) -> Repository:
        """
        Extract repository information and get or create Repository object.
        
        Args:
            repo_id: Repository ID
            repo_name: Repository name
            
        Returns:
            Repository: SQLAlchemy Repository object
        """
        repo_id = int(repo_id)
        
        if repo_id not in self._repo_cache:
            try:
                repo = self.session.query(Repository).get(repo_id)
                if not repo:
                    # Parse owner/name from full_name
                    name_parts = repo_name.split('/')
                    owner = name_parts[0] if len(name_parts) > 1 else None
                    name = name_parts[-1]
                    
                    repo = Repository(
                        id=repo_id,
                        name=name,
                        full_name=repo_name,
                        created_at=datetime.now()
                    )
                    self.session.add(repo)
                self._repo_cache[repo_id] = repo
            except Exception as e:
                logger.error(f"Error getting/creating repository {repo_id}: {e}")
                raise
                
        return self._repo_cache[repo_id]
        
    def _extract_user(self, user_id: int, user_login: str) -> User:
        """
        Extract user information and get or create User object.
        
        Args:
            user_id: User ID
            user_login: User login
            
        Returns:
            User: SQLAlchemy User object
        """
        if user_id not in self._user_cache:
            try:
                user = self.session.query(User).get(user_id)
                if not user:
                    user = User(
                        id=user_id,
                        login=user_login,
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
            if isinstance(timestamp_str, str):
                return datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc)
            else:
                return timestamp_str
        except Exception as e:
            logger.warning(f"Error parsing timestamp {timestamp_str}: {e}")
            return None

    def _create_event(self, event: Dict) -> Event:
        """Create Event object from event dictionary."""
        repo = self._extract_repository(event['id'], event['name'])
        actor = self._extract_user(event['actor_id'], event['actor_login'])
        
        event_obj = Event(
            event_id=str(uuid.uuid4()),
            type=event['type'],
            actor_id=actor.id,
            repo_id=repo.id,
            payload=event.get('payload', {}),
            created_at=self._extract_timestamp(event, 'created_at')
        )
        
        return event_obj

    def map_event(self, event: Dict) -> Event:
        """Map event from BigQuery to Event object."""
        try:
            # Extract event type and basic fields
            event_type = event.get('type')
            if not event_type:
                logger.warning("Event type missing")
                return None

            # Parse timestamp
            created_at_str = event.get('created_at')
            created_at = None
            if created_at_str:
                try:
                    if isinstance(created_at_str, str):
                        created_at = datetime.strptime(created_at_str, '%Y-%m-%d %H:%M:%S%z')
                    else:
                        created_at = created_at_str
                except ValueError as e:
                    logger.warning(f"Error parsing timestamp {created_at_str}: {e}")

            # Initialize payload based on event type
            payload = {
                'push': None,
                'pull_request': None,
                'issue': None,
                'forkee': None,
                'watch': None
            }

            # Map payload based on event type
            if event_type == 'PushEvent':
                payload_data = event.get('payload', {})
                ref = payload_data.get('ref')
                commits = payload_data.get('commits', [])
                
                if not ref or not commits:
                    logger.warning("Missing payload fields for PushEvent: commits, ref")
                
                payload['push'] = {
                    'ref': ref,
                    'commits': [
                        {
                            'sha': commit.get('sha'),
                            'message': commit.get('message'),
                            'author_name': commit.get('author', {}).get('name'),
                            'author_email': commit.get('author', {}).get('email')
                        }
                        for commit in commits
                    ] if commits else []
                }

            elif event_type == 'PullRequestEvent':
                payload_data = event.get('payload', {})
                action = payload_data.get('action')
                pr_data = payload_data.get('pull_request', {})
                
                if not action or not pr_data:
                    logger.warning("Missing payload fields for PullRequestEvent: action, pull_request")
                
                payload['pull_request'] = {
                    'action': action,
                    'pull_request': {
                        'number': pr_data.get('number'),
                        'title': pr_data.get('title'),
                        'body': pr_data.get('body'),
                        'state': pr_data.get('state')
                    }
                }

            elif event_type == 'IssuesEvent':
                payload_data = event.get('payload', {})
                action = payload_data.get('action')
                issue_data = payload_data.get('issue', {})
                
                if not action or not issue_data:
                    logger.warning("Missing payload fields for IssuesEvent: action, issue")
                
                payload['issue'] = {
                    'action': action,
                    'issue': {
                        'number': issue_data.get('number'),
                        'title': issue_data.get('title'),
                        'body': issue_data.get('body'),
                        'state': issue_data.get('state')
                    }
                }

            elif event_type == 'ForkEvent':
                payload_data = event.get('payload', {})
                forkee_data = payload_data.get('forkee', {})
                
                if not forkee_data:
                    logger.warning("Missing payload fields for ForkEvent: forkee")
                
                payload['forkee'] = {
                    'id': forkee_data.get('id'),
                    'full_name': forkee_data.get('full_name')
                }

            elif event_type == 'WatchEvent':
                payload_data = event.get('payload', {})
                action = payload_data.get('action')
                
                if not action:
                    logger.warning("Missing payload fields for WatchEvent: action")
                
                payload['watch'] = {
                    'action': action
                }

            # Create Event object with repository ID from event data
            repo = self._extract_repository(str(event['id']), event['name'])
            actor = self._extract_user(event['actor_id'], event['actor_login'])
            
            # Generate a unique event ID
            event_id = str(uuid.uuid4())
            
            return Event(
                event_id=event_id,
                type=event_type,
                actor_id=actor.id,
                repo_id=repo.id,
                created_at=created_at,
                public=True,
                payload=payload
            )

        except Exception as e:
            logger.error(f"Error mapping event: {e}")
            raise

    def map_event_to_entity(self, event: Dict, entity_class: Type) -> Any:
        """Map event data to specified entity class."""
        try:
            if entity_class == Event:
                return self.map_event(event)
            else:
                logger.error(f"Unsupported entity class: {entity_class}")
                return None
        except Exception as e:
            logger.error(f"Error mapping event to entity: {e}")
            return None
