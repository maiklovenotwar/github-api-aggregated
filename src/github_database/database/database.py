"""Database models and utilities."""

from datetime import datetime
import json
from sqlalchemy import create_engine, Column, Integer, String, DateTime, ForeignKey, Table, Boolean, Text, UniqueConstraint, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.types import TypeDecorator, Text
import os
from typing import List, Optional
from dataclasses import dataclass
from uuid import UUID

Base = declarative_base()

# Default database path
DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'github.db')
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# Custom JSON type that works with both SQLite and MySQL
class JSONType(TypeDecorator):
    """Represents an immutable structure as a json-encoded string."""

    impl = Text
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is not None:
            value = json.dumps(value)
        return value

    def process_result_value(self, value, dialect):
        if value is not None:
            value = json.loads(value)
        return value

# Association tables
contributor_repository = Table(
    'contributor_repository', Base.metadata,
    Column('user_id', Integer, ForeignKey('users.id')),
    Column('repository_id', Integer, ForeignKey('repositories.id'))
)

@dataclass
class CommitData:
    """Git commit data."""
    sha: str
    message: str
    author_name: str
    author_email: str

@dataclass
class PushEventData:
    """Push event data."""
    ref: str
    commits: List[CommitData]

@dataclass
class PullRequestEventData:
    """Pull request event data."""
    action: str
    number: int
    title: str
    body: str
    state: str

@dataclass
class IssueEventData:
    """Issue event data."""
    action: str
    number: int
    title: str
    body: str
    state: str

@dataclass
class ForkEventData:
    """Fork event data."""
    fork_id: int
    fork_name: str

@dataclass
class WatchEventData:
    """Watch event data."""
    action: str

@dataclass
class EventData:
    """Base event class."""
    id: str
    type: str
    actor_id: int
    actor_login: str
    repository_id: int
    repository_name: str
    created_at: datetime
    push_data: Optional[PushEventData] = None
    pull_request_data: Optional[PullRequestEventData] = None
    issue_data: Optional[IssueEventData] = None
    fork_data: Optional[ForkEventData] = None
    watch_data: Optional[WatchEventData] = None

class Event(Base):
    """GitHub event model."""
    
    __tablename__ = 'events'
    
    id = Column(Integer, primary_key=True)
    event_id = Column(String, nullable=False, unique=True)  # Now using UUID-based IDs
    type = Column(String)
    actor_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    repo_id = Column(Integer, ForeignKey('repositories.id'))
    payload = Column(JSONType)
    public = Column(Boolean, default=True)
    created_at = Column(DateTime)
    
    actor = relationship('User', foreign_keys=[actor_id], back_populates='events')
    repository = relationship('Repository', foreign_keys=[repo_id], back_populates='events')

    __table_args__ = (
        UniqueConstraint('event_id', 'actor_id', name='uix_event_actor'),
        Index('ix_events_event_id', 'event_id'),
        Index('ix_events_actor_id', 'actor_id'),
        Index('ix_events_repo_id', 'repo_id'),
        Index('ix_events_created_at', 'created_at'),
    )

class User(Base):
    """User model."""
    
    __tablename__ = 'users'
    
    id = Column(Integer, primary_key=True)
    login = Column(String, unique=True)
    name = Column(String)
    email = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    repositories = relationship('Repository', back_populates='owner')
    contributions = relationship('Repository', secondary=contributor_repository, back_populates='contributors', overlaps="repositories")
    events = relationship('Event', back_populates='actor', foreign_keys='Event.actor_id')
    authored_commits = relationship('Commit', back_populates='user')
    authored_pull_requests = relationship('PullRequest', back_populates='user')
    authored_issues = relationship('Issue', back_populates='user')
    watches = relationship('Watch', back_populates='user')
    forks = relationship('Fork', back_populates='user')
    stars = relationship('Star', back_populates='user')

class Repository(Base):
    """Repository model."""
    
    __tablename__ = 'repositories'
    
    id = Column(Integer, primary_key=True)
    name = Column(String)
    full_name = Column(String, unique=True)
    description = Column(String)
    language = Column(String)
    stars_count = Column(Integer, default=0)
    forks_count = Column(Integer, default=0)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)
    owner_id = Column(Integer, ForeignKey('users.id'))
    
    # Relationships
    owner = relationship('User', back_populates='repositories')
    contributors = relationship('User', secondary=contributor_repository, back_populates='contributions', overlaps="repositories")
    events = relationship('Event', back_populates='repository', foreign_keys='Event.repo_id')
    commits = relationship('Commit', back_populates='repository')
    pull_requests = relationship('PullRequest', back_populates='repository')
    issues = relationship('Issue', back_populates='repository')
    watches = relationship('Watch', back_populates='repository')
    forks = relationship('Fork', back_populates='repository', foreign_keys='Fork.parent_id')
    stars = relationship('Star', back_populates='repository')

class Fork(Base):
    """Fork model."""
    
    __tablename__ = 'forks'
    
    id = Column(Integer, primary_key=True)
    repository_id = Column(Integer, ForeignKey('repositories.id'))  # The forked repository
    parent_id = Column(Integer, ForeignKey('repositories.id'))  # The source repository
    user_id = Column(Integer, ForeignKey('users.id'))
    forked_at = Column(DateTime, default=datetime.utcnow)
    
    repository = relationship('Repository', foreign_keys=[repository_id])
    parent = relationship('Repository', foreign_keys=[parent_id], back_populates='forks')
    user = relationship('User', back_populates='forks')

class Star(Base):
    """Star model."""
    
    __tablename__ = 'stars'
    
    id = Column(Integer, primary_key=True)
    repository_id = Column(Integer, ForeignKey('repositories.id'))
    user_id = Column(Integer, ForeignKey('users.id'))
    starred_at = Column(DateTime, default=datetime.utcnow)
    
    repository = relationship('Repository', back_populates='stars')
    user = relationship('User', back_populates='stars')

class Commit(Base):
    """Commit model."""
    
    __tablename__ = 'commits'
    
    id = Column(Integer, primary_key=True)
    sha = Column(String, unique=True)
    message = Column(Text)
    author_name = Column(String)
    author_email = Column(String)
    timestamp = Column(DateTime)
    repository_id = Column(Integer, ForeignKey('repositories.id'))
    user_id = Column(Integer, ForeignKey('users.id'))
    
    repository = relationship('Repository', back_populates='commits')
    user = relationship('User', back_populates='authored_commits')

class PullRequest(Base):
    """Pull Request model."""
    
    __tablename__ = 'pull_requests'
    
    id = Column(Integer, primary_key=True)
    number = Column(Integer)
    title = Column(String)
    body = Column(Text)
    state = Column(String)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)
    merged_at = Column(DateTime)
    head_ref = Column(String)
    base_ref = Column(String)
    additions = Column(Integer)
    deletions = Column(Integer)
    changed_files = Column(Integer)
    repository_id = Column(Integer, ForeignKey('repositories.id'))
    user_id = Column(Integer, ForeignKey('users.id'))
    
    repository = relationship('Repository', back_populates='pull_requests')
    user = relationship('User', back_populates='authored_pull_requests')

class Issue(Base):
    """Issue model."""
    
    __tablename__ = 'issues'
    
    id = Column(Integer, primary_key=True)
    number = Column(Integer)
    title = Column(String)
    body = Column(Text)
    state = Column(String)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)
    closed_at = Column(DateTime)
    repository_id = Column(Integer, ForeignKey('repositories.id'))
    user_id = Column(Integer, ForeignKey('users.id'))
    
    repository = relationship('Repository', back_populates='issues')
    user = relationship('User', back_populates='authored_issues')

class Watch(Base):
    """Watch model."""
    
    __tablename__ = 'watches'
    
    id = Column(Integer, primary_key=True)
    repository_id = Column(Integer, ForeignKey('repositories.id'))
    user_id = Column(Integer, ForeignKey('users.id'))
    watched_at = Column(DateTime, default=datetime.utcnow)
    
    repository = relationship('Repository', back_populates='watches')
    user = relationship('User', back_populates='watches')

class Database:
    """Database connection and operations."""
    
    def __init__(self, url=None):
        """Initialize database with connection URL."""
        if url is None:
            url = f'sqlite:///{DB_PATH}'
        self.engine = create_engine(url)
        self.Session = sessionmaker(bind=self.engine)
        
    def create_tables(self):
        """Create all database tables."""
        Base.metadata.create_all(self.engine)
        
    def get_session(self):
        """Get a new session."""
        return self.Session()

def create_tables(url=None):
    """Create all database tables."""
    if url is None:
        url = f'sqlite:///{DB_PATH}'
    engine = create_engine(url)
    Base.metadata.create_all(engine)

def get_session(url=None):
    """Get a new database session."""
    if url is None:
        url = f'sqlite:///{DB_PATH}'
    engine = create_engine(url)
    Session = sessionmaker(bind=engine)
    return Session()

def init_db(url=None):
    """Initialize database and create all tables."""
    create_tables(url)
    return get_session(url)

def create_repository_from_api(api_data: dict) -> Repository:
    """
    Create a Repository object from GitHub API data.
    
    Args:
        api_data (dict): Repository data from GitHub API
        
    Returns:
        Repository: New Repository object
    """
    repo = Repository(
        id=api_data['id'],
        name=api_data['name'],
        full_name=api_data['full_name'],
        description=api_data.get('description'),
        language=api_data.get('language'),
        stars_count=api_data.get('stargazers_count', 0),
        forks_count=api_data.get('forks_count', 0),
        created_at=datetime.strptime(api_data['created_at'], '%Y-%m-%dT%H:%M:%SZ'),
        updated_at=datetime.strptime(api_data['updated_at'], '%Y-%m-%dT%H:%M:%SZ'),
        owner_id=api_data['owner']['id']
    )
    return repo

def create_user_from_api(api_data: dict) -> User:
    """
    Create a User object from GitHub API data.
    
    Args:
        api_data (dict): User data from GitHub API
        
    Returns:
        User: New User object
    """
    user = User(
        id=api_data['id'],
        login=api_data['login'],
        name=api_data.get('name'),
        email=api_data.get('email'),
        created_at=datetime.strptime(api_data['created_at'], '%Y-%m-%dT%H:%M:%SZ') if 'created_at' in api_data else None,
        updated_at=datetime.strptime(api_data['updated_at'], '%Y-%m-%dT%H:%M:%SZ') if 'updated_at' in api_data else None
    )
    return user

def create_event_from_api(api_data: dict) -> Event:
    """
    Create an Event object from GitHub API data.
    
    Args:
        api_data (dict): Event data from GitHub API
        
    Returns:
        Event: New Event object
    """
    event = Event(
        event_id=str(UUID(api_data['id'])),  # Convert to UUID-based ID
        type=api_data['type'],
        actor_id=api_data['actor']['id'],
        repo_id=api_data['repo']['id'],
        payload=api_data['payload'],
        public=api_data['public'],
        created_at=datetime.strptime(api_data['created_at'], '%Y-%m-%dT%H:%M:%SZ')
    )
    return event