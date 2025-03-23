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
    CommitData,
    Organization
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
        self._org_cache = {}
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
        
    def _extract_user(self, user_id: int, user_login: str, enriched_data: Optional[Dict] = None) -> User:
        """
        Extract user information and get or create User object.
        
        Args:
            user_id: User ID
            user_login: User login
            enriched_data: Optional enriched user data from the enricher
            
        Returns:
            User: SQLAlchemy User object
        """
        try:
            # Always query the database to get the latest state
            user = self.session.query(User).get(user_id)
            
            # Create base user data
            user_data = {
                'id': user_id,
                'login': user_login,
            }
            
            # Add enriched data if available
            if enriched_data:
                location_data = enriched_data.get('location_data', {})
                user_data.update({
                    'name': enriched_data.get('name'),
                    'email': enriched_data.get('email'),
                    'bio': enriched_data.get('bio'),
                    'company': enriched_data.get('company'),
                    'blog': enriched_data.get('blog'),
                    'location': enriched_data.get('location'),
                    'location_lat': location_data.get('latitude'),
                    'location_lon': location_data.get('longitude'),
                    'location_country': location_data.get('country'),
                    'location_city': location_data.get('city'),
                    'type': enriched_data.get('type'),
                    'site_admin': enriched_data.get('site_admin', False),
                    'hireable': enriched_data.get('hireable'),
                    'public_repos': enriched_data.get('public_repos'),
                    'public_gists': enriched_data.get('public_gists'),
                    'followers': enriched_data.get('followers'),
                    'following': enriched_data.get('following'),
                    'updated_at': enriched_data.get('updated_at') or datetime.now(timezone.utc)
                })
            
            if user is None:
                # Create new user
                user_data['created_at'] = datetime.now(timezone.utc)
                user = User(**user_data)
                self.session.add(user)
            else:
                # Update existing user
                for key, value in user_data.items():
                    if value is not None:  # Only update non-None values
                        setattr(user, key, value)
            
            # Update cache with latest version
            self._user_cache[user_id] = user
            
        except Exception as e:
            logger.error(f"Error getting/creating user {user_id}: {e}")
            raise
            
        return self._user_cache[user_id]
        
    def _extract_organization(self, org_id: int, org_login: str) -> Organization:
        """
        Extract organization information and get or create Organization object.
        
        Args:
            org_id: Organization ID
            org_login: Organization login
            
        Returns:
            Organization: SQLAlchemy Organization object
        """
        try:
            # Always query the database to get the latest state
            org = self.session.query(Organization).get(org_id)
            
            # Create base organization data
            org_data = {
                'id': org_id,
                'login': org_login,
            }
            
            if org is None:
                # Create new organization
                org_data['created_at'] = datetime.now(timezone.utc)
                org = Organization(**org_data)
                self.session.add(org)
            else:
                # Update existing organization
                for key, value in org_data.items():
                    if value is not None:  # Only update non-None values
                        setattr(org, key, value)
            
            # Update cache with latest version
            self._org_cache[org_id] = org
            
        except Exception as e:
            logger.error(f"Error getting/creating organization {org_id}: {e}")
            raise
            
        return self._org_cache[org_id]
        
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
        org = self._extract_organization(event['org_id'], event['org_login']) if 'org_id' in event else None
        
        event_obj = Event(
            event_id=str(uuid.uuid4()),
            type=event['type'],
            actor_id=actor.id,
            repo_id=repo.id,
            org_id=org.id if org else None,
            payload=event.get('payload', {}),
            created_at=self._extract_timestamp(event, 'created_at')
        )
        
        return event_obj

    def create_repository_from_api(self, api_data: Dict) -> Repository:
        """
        Create or update Repository from GitHub API data.
        
        Args:
            api_data: Repository data from GitHub API
            
        Returns:
            Repository: SQLAlchemy Repository object
        """
        repo_id = api_data['id']
        
        try:
            # Get or create repository
            repo = self.session.query(Repository).get(repo_id)
            if not repo:
                repo = Repository(id=repo_id)
                self.session.add(repo)
                
            # Update repository data
            repo.name = api_data['name']
            repo.full_name = api_data['full_name']
            repo.description = api_data.get('description')
            repo.language = api_data.get('language')
            repo.stars_count = api_data.get('stargazers_count', 0)
            repo.forks_count = api_data.get('forks_count', 0)
            repo.watchers_count = api_data.get('watchers_count', 0)
            repo.open_issues_count = api_data.get('open_issues_count', 0)
            repo.is_fork = api_data.get('fork', False)
            repo.is_archived = api_data.get('archived', False)
            repo.is_disabled = api_data.get('disabled', False)
            repo.license_key = api_data.get('license', {}).get('key')
            repo.default_branch = api_data.get('default_branch', 'master')
            repo.has_issues = api_data.get('has_issues', True)
            repo.has_projects = api_data.get('has_projects', True)
            repo.has_wiki = api_data.get('has_wiki', True)
            repo.created_at = parser.parse(api_data['created_at'])
            repo.updated_at = parser.parse(api_data['updated_at'])
            repo.pushed_at = parser.parse(api_data.get('pushed_at')) if api_data.get('pushed_at') else None
            
            # Handle owner (User or Organization)
            owner_data = api_data.get('owner', {})
            if owner_data:
                owner_type = owner_data.get('type')
                owner_id = owner_data.get('id')
                owner_login = owner_data.get('login')
                
                if owner_type == 'Organization':
                    # Get or create organization
                    org = self._extract_organization(owner_id, owner_login)
                    repo.organization = org
                    repo.owner_type = 'Organization'
                else:
                    # Get or create user
                    user = self._extract_user(owner_id, owner_login)
                    repo.owner = user
                    repo.owner_type = 'User'
                    
            # Update topics
            if 'topics' in api_data:
                repo.topics = api_data['topics']
                
            # Cache repository
            self._repo_cache[repo_id] = repo
            return repo
            
        except Exception as e:
            logger.error(f"Error creating repository from API data: {e}")
            raise

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
                'watch': None,
                'organization': None
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

            elif event_type == 'OrganizationEvent':
                payload_data = event.get('payload', {})
                action = payload_data.get('action')
                membership = payload_data.get('membership', {})
                invitation = payload_data.get('invitation', {})
                
                payload['organization'] = {
                    'action': action,
                    'membership': {
                        'role': membership.get('role'),
                        'scope': membership.get('scope'),
                        'team_name': membership.get('team', {}).get('name')
                    } if membership else {},
                    'invitation': {
                        'id': invitation.get('id'),
                        'role': invitation.get('role'),
                        'created_at': invitation.get('created_at')
                    } if invitation else {}
                }

            # Create event
            event_obj = Event(
                id=event.get('id'),
                type=event_type,
                created_at=created_at,
                public=event.get('public', True),
                payload=payload
            )

            # Extract and link repository
            repo_data = event.get('repo', {})
            if repo_data:
                repo = self._extract_repository(
                    repo_data.get('id'),
                    repo_data.get('name')
                )
                event_obj.repository = repo

            # Extract and link actor (user)
            actor_data = event.get('actor', {})
            if actor_data:
                actor = self._extract_user(
                    actor_data.get('id'),
                    actor_data.get('login')
                )
                event_obj.actor = actor

            # Extract and link organization if present
            org_data = event.get('org', {})
            if org_data:
                org = self._extract_organization(
                    org_data.get('id'),
                    org_data.get('login')
                )
                event_obj.organization = org

            return event_obj

        except Exception as e:
            logger.error(f"Error mapping event: {e}")
            return None

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

    def map_organization_event(self, event: Dict) -> Dict:
        """
        Map organization event to database models.
        
        Args:
            event: Organization event data
            
        Returns:
            Dict: Mapped event data
        """
        # Validate event
        self.validator.validate_event(event)
        
        # Extract organization
        org_data = event.get('organization', {})
        if not org_data:
            raise EventValidationError(
                event_type='OrganizationEvent',
                event_id=event.get('id', 'unknown'),
                reason='Missing organization data'
            )
            
        org = self._extract_organization(
            org_data.get('id'),
            org_data.get('login')
        )
        
        # Extract actor (user)
        actor_data = event.get('actor', {})
        if actor_data:
            actor = self._extract_user(
                actor_data.get('id'),
                actor_data.get('login')
            )
        else:
            actor = None
            
        # Create event
        event_obj = self._create_event(event)
        event_obj.organization = org
        event_obj.actor = actor
        
        # Add membership data if present
        membership = event.get('membership', {})
        if membership:
            event_obj.data = {
                'action': event.get('action'),
                'scope': membership.get('scope'),
                'role': membership.get('role'),
                'team_name': membership.get('team', {}).get('name')
            }
            
        # Add invitation data if present
        invitation = event.get('invitation', {})
        if invitation:
            event_obj.data.update({
                'invitation_id': invitation.get('id'),
                'invitation_role': invitation.get('role'),
                'invitation_created_at': invitation.get('created_at')
            })
            
        return event_obj.to_dict()
