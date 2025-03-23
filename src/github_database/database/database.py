"""Database models for GitHub data."""

from datetime import datetime
from typing import Optional, List
import os
import logging

from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime,
    ForeignKey, Text, create_engine
)
from sqlalchemy.orm import relationship, declarative_base, sessionmaker

logger = logging.getLogger(__name__)

Base = declarative_base()

class User(Base):
    """User model."""
    __tablename__ = 'users'
    
    id = Column(Integer, primary_key=True)
    login = Column(String, nullable=False, unique=True)
    name = Column(String)
    email = Column(String)
    type = Column(String)
    avatar_url = Column(String)
    company = Column(String)
    blog = Column(String)
    location = Column(String)
    bio = Column(Text)
    twitter_username = Column(String)
    public_repos = Column(Integer)
    public_gists = Column(Integer)
    followers = Column(Integer)
    following = Column(Integer)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)
    
    # Relationships
    repositories = relationship('Repository', back_populates='owner')
    organizations = relationship('Organization', secondary='organization_members')

class Organization(Base):
    """Organization model."""
    __tablename__ = 'organizations'
    
    id = Column(Integer, primary_key=True)
    login = Column(String, nullable=False, unique=True)
    name = Column(String)
    email = Column(String)
    type = Column(String)
    avatar_url = Column(String)
    company = Column(String)
    blog = Column(String)
    location = Column(String)
    bio = Column(Text)
    twitter_username = Column(String)
    public_repos = Column(Integer)
    public_gists = Column(Integer)
    followers = Column(Integer)
    following = Column(Integer)
    public_members = Column(Integer)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)
    
    # Relationships
    repositories = relationship('Repository', back_populates='organization')
    members = relationship('User', secondary='organization_members')

class OrganizationMember(Base):
    """Organization member model."""
    __tablename__ = 'organization_members'
    
    organization_id = Column(Integer, ForeignKey('organizations.id'), primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'), primary_key=True)
    joined_at = Column(DateTime, default=datetime.utcnow)

class Repository(Base):
    """Repository model."""
    __tablename__ = 'repositories'
    
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    full_name = Column(String, nullable=False, unique=True)
    owner_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    organization_id = Column(Integer, ForeignKey('organizations.id'))
    
    # Metadata
    description = Column(Text)
    homepage = Column(String)
    language = Column(String)
    private = Column(Boolean, default=False)
    fork = Column(Boolean, default=False)
    default_branch = Column(String)
    size = Column(Integer)
    
    # Stats
    stargazers_count = Column(Integer, default=0)
    watchers_count = Column(Integer, default=0)
    forks_count = Column(Integer, default=0)
    open_issues_count = Column(Integer, default=0)
    
    # BigQuery metrics
    stars = Column(Integer, default=0)
    contributors = Column(Integer, default=0)
    commits = Column(Integer, default=0)
    
    # Timestamps
    created_at = Column(DateTime)
    updated_at = Column(DateTime)
    pushed_at = Column(DateTime)
    
    # Relationships
    owner = relationship('User', back_populates='repositories')
    organization = relationship('Organization', back_populates='repositories')
    events = relationship('Event', back_populates='repository')

class Event(Base):
    """Event model."""
    __tablename__ = 'events'
    
    id = Column(String, primary_key=True)
    type = Column(String, nullable=False)
    actor_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    repository_id = Column(Integer, ForeignKey('repositories.id'), nullable=False)
    created_at = Column(DateTime, nullable=False)
    
    # Relationships
    actor = relationship('User')
    repository = relationship('Repository', back_populates='events')

def init_db(database_url: str) -> sessionmaker:
    """Initialize database."""
    # Wenn SQLite-Datenbank, prüfe ob die Datei existiert und lösche sie ggf.
    if database_url.startswith('sqlite:///'):
        db_path = database_url.replace('sqlite:///', '')
        if os.path.exists(db_path):
            try:
                logger.info(f"Removing existing database file: {db_path}")
                os.remove(db_path)
                logger.info("Existing database file removed successfully")
            except Exception as e:
                logger.error(f"Error removing database file: {e}")
    
    # Erstelle Engine und Tabellen
    engine = create_engine(database_url)
    logger.info("Creating database tables...")
    Base.metadata.create_all(engine)
    logger.info("Database tables created successfully")
    
    # Erstelle und gib Session-Factory zurück
    return sessionmaker(bind=engine)