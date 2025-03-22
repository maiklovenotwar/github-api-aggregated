"""Database models and utilities."""

from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, DateTime, ForeignKey, Table, Boolean, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.types import TypeDecorator, JSON
import os

Base = declarative_base()

# Default database path
DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'github.db')
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# Custom JSON type that works with SQLite
class JSONType(TypeDecorator):
    """Represents an immutable structure as a json-encoded string."""

    impl = Text
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is not None:
            value = JSON().process_bind_param(value, dialect)
        return value

    def process_result_value(self, value, dialect):
        if value is not None:
            value = JSON().process_result_value(value, dialect)
        return value

# Association tables
contributor_repository = Table(
    'contributor_repository', Base.metadata,
    Column('user_id', Integer, ForeignKey('users.id')),
    Column('repository_id', Integer, ForeignKey('repositories.id'))
)

class Event(Base):
    """GitHub event model."""
    
    __tablename__ = 'events'
    
    id = Column(Integer, primary_key=True)
    event_id = Column(String, unique=True)
    type = Column(String)
    actor_id = Column(Integer, ForeignKey('users.id'))
    repo_id = Column(Integer, ForeignKey('repositories.id'))
    payload = Column(JSONType)
    public = Column(Boolean, default=True)
    created_at = Column(DateTime)
    
    actor = relationship('User', foreign_keys=[actor_id], back_populates='events')
    repository = relationship('Repository', foreign_keys=[repo_id], back_populates='events')

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
    watched_at = Column(DateTime)
    
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

def create_repository_from_api(api_data):
    """
    Create a Repository object from GitHub API data.
    
    Args:
        api_data (dict): Repository data from GitHub API
        
    Returns:
        Repository: New Repository object
    """
    return Repository(
        id=api_data['id'],
        name=api_data['name'],
        full_name=api_data['full_name'],
        description=api_data.get('description'),
        language=api_data.get('language'),
        stars_count=api_data.get('stargazers_count', 0),
        forks_count=api_data.get('forks_count', 0),
        created_at=api_data.get('created_at'),
        updated_at=api_data.get('updated_at')
    )

def create_user_from_api(api_data):
    """
    Create a User object from GitHub API data.
    
    Args:
        api_data (dict): User data from GitHub API
        
    Returns:
        User: New User object
    """
    return User(
        id=api_data['id'],
        login=api_data['login'],
        name=api_data.get('name'),
        email=api_data.get('email'),
        created_at=api_data.get('created_at'),
        updated_at=api_data.get('updated_at')
    )

def create_event_from_api(api_data):
    """
    Create an Event object from GitHub API data.
    
    Args:
        api_data (dict): Event data from GitHub API
        
    Returns:
        Event: New Event object
    """
    return Event(
        event_id=api_data['id'],
        type=api_data['type'],
        actor_id=api_data['actor']['id'] if api_data.get('actor') else None,
        repo_id=api_data['repo']['id'] if api_data.get('repo') else None,
        payload=api_data.get('payload', {}),
        public=api_data.get('public', True),
        created_at=api_data.get('created_at')
    )