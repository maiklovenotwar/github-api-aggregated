"""Repository mapper for converting GitHub Archive events to database models."""

import logging
from datetime import datetime
from typing import Dict, List, Optional, Set, Type, Union, Tuple
from dataclasses import dataclass
from pathlib import Path
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

from ..config import ETLConfig
from ..database.database import (
    Repository,
    User,
    Commit,
    PullRequest,
    Issue,
    Event,
    Fork,
    Star,
    Watch
)
from ..enrichment.data_enricher import DataEnricher

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class EventValidationError(Exception):
    """Raised when event validation fails."""
    event_type: str
    event_id: str
    reason: str

class EventValidator:
    """Validate GitHub Archive events."""
    
    REQUIRED_FIELDS = {
        'id', 'type', 'actor', 'repo', 'created_at'
    }
    
    PAYLOAD_SCHEMAS = {
        'PushEvent': {'ref', 'commits'},
        'PullRequestEvent': {'action', 'pull_request'},
        'IssuesEvent': {'action', 'issue'},
        'ForkEvent': {'forkee'},
        'WatchEvent': {'action'},
        'StarEvent': {'action'}
    }
    
    def validate_event(self, event_dict: Dict) -> None:
        """
        Validate event structure and required fields.
        
        Args:
            event_dict: Raw event dictionary from GitHub Archive
            
        Raises:
            EventValidationError: If validation fails
        """
        # Check required fields
        missing_fields = self.REQUIRED_FIELDS - set(event_dict.keys())
        if missing_fields:
            raise EventValidationError(
                event_type=event_dict.get('type', 'unknown'),
                event_id=event_dict.get('id', 'unknown'),
                reason=f"Missing required fields: {missing_fields}"
            )
            
        # Validate payload schema
        event_type = event_dict['type']
        if event_type in self.PAYLOAD_SCHEMAS:
            payload = event_dict.get('payload', {})
            required_payload_fields = self.PAYLOAD_SCHEMAS[event_type]
            missing_payload_fields = required_payload_fields - set(payload.keys())
            
            if missing_payload_fields:
                raise EventValidationError(
                    event_type=event_type,
                    event_id=event_dict['id'],
                    reason=f"Missing payload fields: {missing_payload_fields}"
                )

class RepositoryMapper:
    """Map GitHub Archive events to database models."""
    
    def __init__(self, session: Session, config: ETLConfig):
        self.session = session
        self.config = config
        self.validator = EventValidator()
        
        # Initialize data enricher
        cache_dir = Path(config.database.url).parent / 'cache'
        cache_dir.mkdir(exist_ok=True)
        self.enricher = DataEnricher(config, cache_dir)
        
        # Cache for database objects
        self._repository_cache: Dict[int, Repository] = {}
        self._user_cache: Dict[int, User] = {}
        
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
        
        if repo_id not in self._repository_cache:
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
                        owner=owner,
                        full_name=repo_dict['name'],
                        description=repo_dict.get('description'),
                        language=repo_dict.get('language'),
                        stars=repo_dict.get('stars', 0),
                        forks=repo_dict.get('forks', 0),
                        created_at=datetime.now()
                    )
                    self.session.add(repo)
                self._repository_cache[repo_id] = repo
            except Exception as e:
                logger.error(f"Error getting/creating repository {repo_id}: {e}")
                raise
                
        return self._repository_cache[repo_id]
        
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
                        company=user_dict.get('company'),
                        location=user_dict.get('location'),
                        created_at=datetime.now()
                    )
                    self.session.add(user)
                self._user_cache[user_id] = user
            except Exception as e:
                logger.error(f"Error getting/creating user {user_id}: {e}")
                raise
                
        return self._user_cache[user_id]
        
    def _extract_timestamp(self, event_dict: Dict, field_name: str) -> Optional[datetime]:
        """
        Extract and convert timestamp from event.
        
        Args:
            event_dict: Event dictionary
            field_name: Name of the timestamp field
            
        Returns:
            Optional[datetime]: Parsed datetime or None if invalid/missing
        """
        try:
            timestamp = event_dict.get(field_name)
            if timestamp:
                return datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            return None
        except (ValueError, AttributeError) as e:
            logger.warning(f"Error parsing timestamp {field_name}: {e}")
            return None
            
    def map_pushevent(self, event_dict: Dict) -> Tuple[Event, List[Commit]]:
        """Map PushEvent to database models."""
        try:
            payload = event_dict['payload']
            repo = self._extract_repository(event_dict['repo'])
            pusher = self._extract_user(event_dict['actor'])
            
            # Create commits
            commits = []
            for commit_data in payload['commits']:
                try:
                    # Enrich commit data
                    commit_data = self.enricher.enrich_commit(
                        commit_data,
                        repo.full_name
                    )
                    
                    author_dict = commit_data['author']
                    author = self._extract_user({
                        'id': author_dict.get('id', 0),
                        'name': author_dict.get('name'),
                        'email': author_dict.get('email')
                    })
                    
                    commit = Commit(
                        sha=commit_data['sha'],
                        message=commit_data['message'],
                        author_id=author.id,
                        repository_id=repo.id,
                        branch=payload.get('ref', '').split('/')[-1],
                        additions=commit_data.get('stats', {}).get('additions', 0),
                        deletions=commit_data.get('stats', {}).get('deletions', 0),
                        created_at=self._extract_timestamp(commit_data, 'timestamp') or 
                                 self._extract_timestamp(event_dict, 'created_at')
                    )
                    commits.append(commit)
                    self.session.add(commit)
                except Exception as e:
                    logger.warning(f"Error processing commit {commit_data.get('sha')}: {e}")
                    continue
                    
            # Create event
            event = Event(
                event_id=event_dict['id'],
                type='PushEvent',
                actor_id=pusher.id,
                repo_id=repo.id,
                payload=str(payload),
                created_at=self._extract_timestamp(event_dict, 'created_at')
            )
            
            return event, commits
        except Exception as e:
            logger.error(f"Error mapping PushEvent: {e}")
            raise
            
    def map_pullrequestevent(self, event_dict: Dict) -> Tuple[Event, PullRequest]:
        """Map PullRequestEvent to database models."""
        try:
            payload = event_dict['payload']
            
            # Enrich pull request data
            pr_data = self.enricher.enrich_pull_request(
                payload['pull_request'],
                event_dict['repo']['name']
            )
            
            repo = self._extract_repository(event_dict['repo'])
            author = self._extract_user(pr_data['user'])
            
            # Create pull request
            pull_request = PullRequest(
                pr_id=pr_data['id'],
                number=pr_data['number'],
                state=pr_data['state'],
                title=pr_data['title'],
                body=pr_data.get('body', ''),
                user_id=author.id,
                repository_id=repo.id,
                base_ref=pr_data['base']['ref'],
                head_ref=pr_data['head']['ref'],
                is_merged=pr_data.get('merged', False),
                review_comments=pr_data.get('review_comments', 0),
                commits_count=pr_data.get('commits', 0),
                additions=pr_data.get('additions', 0),
                deletions=pr_data.get('deletions', 0),
                changed_files=pr_data.get('changed_files', 0),
                created_at=self._extract_timestamp(pr_data, 'created_at'),
                updated_at=self._extract_timestamp(pr_data, 'updated_at'),
                closed_at=self._extract_timestamp(pr_data, 'closed_at'),
                merged_at=self._extract_timestamp(pr_data, 'merged_at')
            )
            self.session.add(pull_request)
            
            # Create event
            event = Event(
                event_id=event_dict['id'],
                type='PullRequestEvent',
                actor_id=author.id,
                repo_id=repo.id,
                payload=str(payload),
                created_at=self._extract_timestamp(event_dict, 'created_at')
            )
            
            return event, pull_request
        except Exception as e:
            logger.error(f"Error mapping PullRequestEvent: {e}")
            raise
            
    def map_issuesevent(self, event_dict: Dict) -> Tuple[Event, Issue]:
        """Map IssuesEvent to database models."""
        try:
            payload = event_dict['payload']
            
            # Enrich issue data
            issue_data = self.enricher.enrich_issue(
                payload['issue'],
                event_dict['repo']['name']
            )
            
            repo = self._extract_repository(event_dict['repo'])
            author = self._extract_user(issue_data['user'])
            
            # Create issue
            issue = Issue(
                issue_id=issue_data['id'],
                number=issue_data['number'],
                state=issue_data['state'],
                title=issue_data['title'],
                body=issue_data.get('body', ''),
                user_id=author.id,
                repository_id=repo.id,
                comments=issue_data.get('comments', 0),
                labels=[label['name'] for label in issue_data.get('labels', [])],
                created_at=self._extract_timestamp(issue_data, 'created_at'),
                updated_at=self._extract_timestamp(issue_data, 'updated_at'),
                closed_at=self._extract_timestamp(issue_data, 'closed_at')
            )
            self.session.add(issue)
            
            # Create event
            event = Event(
                event_id=event_dict['id'],
                type='IssuesEvent',
                actor_id=author.id,
                repo_id=repo.id,
                payload=str(payload),
                created_at=self._extract_timestamp(event_dict, 'created_at')
            )
            
            return event, issue
        except Exception as e:
            logger.error(f"Error mapping IssuesEvent: {e}")
            raise
            
    def map_forkevent(self, event_dict: Dict) -> Tuple[Event, Fork]:
        """Map ForkEvent to database models."""
        try:
            payload = event_dict['payload']
            forkee_data = payload['forkee']
            
            source_repo = self._extract_repository(event_dict['repo'])
            forker = self._extract_user(event_dict['actor'])
            forked_repo = self._extract_repository(forkee_data)
            
            # Create fork
            fork = Fork(
                fork_id=forked_repo.id,
                parent_id=source_repo.id,
                owner_id=forker.id,
                created_at=self._extract_timestamp(event_dict, 'created_at')
            )
            self.session.add(fork)
            
            # Create event
            event = Event(
                event_id=event_dict['id'],
                type='ForkEvent',
                actor_id=forker.id,
                repo_id=source_repo.id,
                payload=str(payload),
                created_at=self._extract_timestamp(event_dict, 'created_at')
            )
            
            return event, fork
        except Exception as e:
            logger.error(f"Error mapping ForkEvent: {e}")
            raise
            
    def map_watchevent(self, event_dict: Dict) -> Tuple[Event, Watch]:
        """Map WatchEvent to database models."""
        try:
            payload = event_dict['payload']
            repo = self._extract_repository(event_dict['repo'])
            user = self._extract_user(event_dict['actor'])
            
            # Create watch
            watch = Watch(
                user_id=user.id,
                repository_id=repo.id,
                created_at=self._extract_timestamp(event_dict, 'created_at')
            )
            
            if payload.get('action') == 'started':
                self.session.add(watch)
            elif payload.get('action') == 'stopped':
                self.session.query(Watch).filter_by(
                    user_id=user.id,
                    repository_id=repo.id
                ).delete()
                
            # Create event
            event = Event(
                event_id=event_dict['id'],
                type='WatchEvent',
                actor_id=user.id,
                repo_id=repo.id,
                payload=str(payload),
                created_at=self._extract_timestamp(event_dict, 'created_at')
            )
            
            return event, watch
        except Exception as e:
            logger.error(f"Error mapping WatchEvent: {e}")
            raise
            
    def map_starevent(self, event_dict: Dict) -> Tuple[Event, Star]:
        """Map StarEvent to database models."""
        try:
            payload = event_dict['payload']
            repo = self._extract_repository(event_dict['repo'])
            user = self._extract_user(event_dict['actor'])
            
            # Create star
            star = Star(
                user_id=user.id,
                repository_id=repo.id,
                created_at=self._extract_timestamp(event_dict, 'created_at')
            )
            
            if payload.get('action') == 'started':
                self.session.add(star)
            elif payload.get('action') == 'stopped':
                self.session.query(Star).filter_by(
                    user_id=user.id,
                    repository_id=repo.id
                ).delete()
                
            # Create event
            event = Event(
                event_id=event_dict['id'],
                type='StarEvent',
                actor_id=user.id,
                repo_id=repo.id,
                payload=str(payload),
                created_at=self._extract_timestamp(event_dict, 'created_at')
            )
            
            return event, star
        except Exception as e:
            logger.error(f"Error mapping StarEvent: {e}")
            raise
