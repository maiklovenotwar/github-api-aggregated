"""ETL orchestrator for GitHub data."""

import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional

from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from .config import ETLConfig
from .api.github_api import GitHubAPIClient, GitHubAPIError, RateLimitError
from .api.bigquery_api import BigQueryClient
from .database.database import User, Organization, Repository, Event

logger = logging.getLogger(__name__)

class ETLOrchestrator:
    """ETL orchestrator for GitHub data."""
    
    def __init__(self, config: ETLConfig):
        """Initialize orchestrator."""
        self.config = config
        self.github_client = GitHubAPIClient(config.github)
        self.bigquery_client = BigQueryClient(config.bigquery)
        
    def _handle_api_error(self, error: Exception, context: str) -> None:
        """Handle API errors."""
        if isinstance(error, RateLimitError):
            wait_time = error.reset_time - datetime.now(timezone.utc).timestamp()
            logger.warning(f"Rate limit exceeded in {context}. Waiting {wait_time:.0f}s...")
            raise error
            
        if isinstance(error, GitHubAPIError):
            if error.status_code == 404:
                logger.warning(f"Not found in {context}: {error}")
                return
            logger.error(f"API error in {context}: {error}")
            raise error
            
        logger.error(f"Unexpected error in {context}: {error}")
        raise error
        
    def _get_or_create_user(self, user_data: Dict[str, Any], session: Session) -> Optional[User]:
        """Get or create a user."""
        try:
            user = session.query(User).filter_by(id=user_data['id']).first()
            if not user:
                user_details = self.github_client.get_user(user_data['login'])
                user = User(
                    id=user_details['id'],
                    login=user_details['login'],
                    name=user_details.get('name'),
                    email=user_details.get('email'),
                    location=user_details.get('location'),
                    company=user_details.get('company'),
                    bio=user_details.get('bio'),
                    blog=user_details.get('blog'),
                    twitter_username=user_details.get('twitter_username'),
                    public_repos=user_details.get('public_repos', 0),
                    public_gists=user_details.get('public_gists', 0),
                    followers=user_details.get('followers', 0),
                    following=user_details.get('following', 0),
                    created_at=datetime.strptime(user_details['created_at'], '%Y-%m-%dT%H:%M:%SZ'),
                    updated_at=datetime.strptime(user_details['updated_at'], '%Y-%m-%dT%H:%M:%SZ')
                )
                session.add(user)
                session.commit()
            return user
        except Exception as e:
            logger.error(f"Error creating user {user_data['login']}: {e}")
            session.rollback()
            return None
            
    def _get_or_create_organization(self, org_data: Dict[str, Any], session: Session) -> Optional[Organization]:
        """Get or create an organization."""
        try:
            org = session.query(Organization).filter_by(id=org_data['id']).first()
            if not org:
                org_details = self.github_client.get_organization(org_data['login'])
                org = Organization(
                    id=org_details['id'],
                    login=org_details['login'],
                    name=org_details.get('name'),
                    bio=org_details.get('description'),  
                    blog=org_details.get('blog'),
                    location=org_details.get('location'),
                    email=org_details.get('email'),
                    twitter_username=org_details.get('twitter_username'),
                    public_repos=org_details.get('public_repos', 0),
                    public_gists=org_details.get('public_gists', 0),
                    followers=org_details.get('followers', 0),
                    following=org_details.get('following', 0),
                    created_at=datetime.strptime(org_details['created_at'], '%Y-%m-%dT%H:%M:%SZ'),
                    updated_at=datetime.strptime(org_details['updated_at'], '%Y-%m-%dT%H:%M:%SZ')
                )
                session.add(org)
                session.commit()
            return org
        except Exception as e:
            logger.error(f"Error creating organization {org_data['login']}: {e}")
            session.rollback()
            return None
            
    def process_repository(self, full_name: str, session: Session) -> Optional[Repository]:
        """Process a repository."""
        try:
            repo = session.query(Repository).filter_by(full_name=full_name).first()
            if not repo:
                # Split the full_name into owner and name
                owner, name = full_name.split('/')
                repo_data = self.github_client.get_repository(owner, name)
                owner_obj = None
                organization_id = None
                
                if repo_data['owner']['type'] == 'Organization':
                    owner_obj = self._get_or_create_organization(repo_data['owner'], session)
                    if owner_obj:
                        organization_id = owner_obj.id
                else:
                    owner_obj = self._get_or_create_user(repo_data['owner'], session)
                    
                if owner_obj:
                    repo = Repository(
                        id=repo_data['id'],
                        name=repo_data['name'],
                        full_name=repo_data['full_name'],
                        owner_id=owner_obj.id,
                        organization_id=organization_id,  # Setze organization_id, wenn der Besitzer eine Organisation ist
                        description=repo_data.get('description'),
                        homepage=repo_data.get('homepage'),
                        language=repo_data.get('language'),
                        private=repo_data.get('private', False),
                        fork=repo_data.get('fork', False),
                        default_branch=repo_data.get('default_branch'),
                        size=repo_data.get('size', 0),
                        stargazers_count=repo_data.get('stargazers_count', 0),  # Setze stargazers_count aus der API
                        watchers_count=repo_data.get('watchers_count', 0),      # Setze watchers_count aus der API
                        forks_count=repo_data.get('forks_count', 0),            # Setze forks_count aus der API
                        open_issues_count=repo_data.get('open_issues_count', 0), # Setze open_issues_count aus der API
                        created_at=datetime.strptime(repo_data['created_at'], '%Y-%m-%dT%H:%M:%SZ'),
                        updated_at=datetime.strptime(repo_data['updated_at'], '%Y-%m-%dT%H:%M:%SZ'),
                        pushed_at=datetime.strptime(repo_data['pushed_at'], '%Y-%m-%dT%H:%M:%SZ') if repo_data.get('pushed_at') else None
                    )
                    session.add(repo)
                    session.commit()
            return repo
        except Exception as e:
            logger.error(f"Error processing repository {full_name}: {e}")
            session.rollback()
            return None
            
    def _process_event(self, event_data: Dict[str, Any], session: Session) -> None:
        """Process a single event."""
        try:
            event = Event(
                id=event_data['id'],
                type=event_data['type'],
                actor_id=event_data['actor']['id'],
                repo_id=event_data['repo']['id'],
                created_at=datetime.strptime(event_data['created_at'], '%Y-%m-%dT%H:%M:%SZ')
            )
            session.add(event)
        except Exception as e:
            logger.error(f"Error processing event {event_data['id']}: {e}")
            raise e
            
    def update_yearly_data(self, year: int, session: Session) -> None:
        """Update data for a specific year."""
        try:
            events = self.bigquery_client.get_events(year)
            for event_data in events:
                self._process_event(event_data, session)
            session.commit()
        except SQLAlchemyError as e:
            logger.error(f"Error in updating data for year {year}: {e}")
            session.rollback()
            raise
        except Exception as e:
            logger.error(f"Error in updating data for year {year}: {e}")
            raise
