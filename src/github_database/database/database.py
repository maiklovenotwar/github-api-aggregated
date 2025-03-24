"""Database models for GitHub data."""

from datetime import datetime
from typing import Optional, List
import os
import logging

from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime,
    ForeignKey, Text, create_engine, Table
)
from sqlalchemy.orm import relationship, declarative_base, sessionmaker

logger = logging.getLogger(__name__)

Base = declarative_base()

# Zwischentabelle für die Beziehung zwischen Contributors und Repositories
contributor_repository = Table(
    'contributor_repository', Base.metadata,
    Column('contributor_id', Integer, ForeignKey('contributors.id'), primary_key=True),
    Column('repository_id', Integer, ForeignKey('repositories.id'), primary_key=True),
    Column('contributions', Integer, default=0),
    Column('first_contribution_at', DateTime),
    Column('last_contribution_at', DateTime)
)

# Zwischentabelle für die Beziehung zwischen Contributors und Organizations
contributor_organization = Table(
    'contributor_organization', Base.metadata,
    Column('contributor_id', Integer, ForeignKey('contributors.id'), primary_key=True),
    Column('organization_id', Integer, ForeignKey('organizations.id'), primary_key=True),
    Column('joined_at', DateTime, default=datetime.utcnow)
)

class Contributor(Base):
    """Contributor model (ersetzt User)."""
    __tablename__ = 'contributors'
    
    id = Column(Integer, primary_key=True)
    login = Column(String, nullable=False, unique=True)
    name = Column(String)
    email = Column(String)
    type = Column(String)
    avatar_url = Column(String)
    company = Column(String)
    blog = Column(String)
    location = Column(String)
    country_code = Column(String(2))  # ISO-Code für das Land
    region = Column(String)  # Region innerhalb des Landes oder Kontinent
    bio = Column(Text)
    twitter_username = Column(String)
    public_repos = Column(Integer)
    public_gists = Column(Integer)
    followers = Column(Integer)
    following = Column(Integer)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)
    
    # Beziehungen
    repositories = relationship('Repository', secondary=contributor_repository, back_populates='contributors')
    organizations = relationship('Organization', secondary=contributor_organization, back_populates='contributors')

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
    country_code = Column(String(2))  # ISO-Code für das Land
    region = Column(String)  # Region innerhalb des Landes oder Kontinent
    bio = Column(Text)
    twitter_username = Column(String)
    public_repos = Column(Integer)
    public_gists = Column(Integer)
    followers = Column(Integer)
    following = Column(Integer)
    public_members = Column(Integer)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)
    
    # Beziehungen
    repositories = relationship('Repository', back_populates='organization')
    contributors = relationship('Contributor', secondary=contributor_organization, back_populates='organizations')
    
    # Aggregierte Statistiken nach Jahren
    yearly_stats = relationship('OrganizationYearlyStats', back_populates='organization')

class Repository(Base):
    """Repository model."""
    __tablename__ = 'repositories'
    
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    full_name = Column(String, nullable=False, unique=True)
    owner_id = Column(Integer, ForeignKey('contributors.id'), nullable=False)
    organization_id = Column(Integer, ForeignKey('organizations.id'))
    
    # Metadata
    description = Column(Text)
    homepage = Column(String)
    language = Column(String)
    private = Column(Boolean, default=False)
    fork = Column(Boolean, default=False)
    default_branch = Column(String)
    size = Column(Integer)
    
    # Stats von GitHub API
    stargazers_count = Column(Integer, default=0)
    watchers_count = Column(Integer, default=0)
    forks_count = Column(Integer, default=0)
    open_issues_count = Column(Integer, default=0)
    
    # BigQuery metrics
    stars = Column(Integer, default=0)
    forks = Column(Integer, default=0)
    contributors_count = Column(Integer, default=0)
    commits = Column(Integer, default=0)
    
    # Timestamps
    created_at = Column(DateTime)
    updated_at = Column(DateTime)
    pushed_at = Column(DateTime)
    
    # Beziehungen
    owner = relationship('Contributor', foreign_keys=[owner_id])
    organization = relationship('Organization', back_populates='repositories')
    contributors = relationship('Contributor', secondary=contributor_repository, back_populates='repositories')

class OrganizationYearlyStats(Base):
    """Aggregated yearly statistics for organizations."""
    __tablename__ = 'organization_yearly_stats'
    
    id = Column(Integer, primary_key=True)
    year = Column(Integer, nullable=False, index=True)
    organization_id = Column(Integer, ForeignKey('organizations.id'), nullable=False, index=True)
    location = Column(String)
    country_code = Column(String(2), index=True)  # ISO-Code
    region = Column(String)  # Region oder Kontinent
    forks = Column(Integer, default=0)
    stars = Column(Integer, default=0)
    number_repos = Column(Integer, default=0)
    number_commits = Column(Integer, default=0)
    number_contributors = Column(Integer, default=0)
    
    # Beziehungen
    organization = relationship('Organization', back_populates='yearly_stats')
    
    __table_args__ = (
        # Composite unique constraint for year and organization
        {'sqlite_autoincrement': True},
    )

class CountryYearlyStats(Base):
    """Aggregated yearly statistics by country."""
    __tablename__ = 'country_yearly_stats'
    
    id = Column(Integer, primary_key=True)
    year = Column(Integer, nullable=False, index=True)
    country_code = Column(String(2), nullable=False, index=True)  # ISO-Code
    region = Column(String)  # Region oder Kontinent
    forks = Column(Integer, default=0)
    stars = Column(Integer, default=0)
    number_repos = Column(Integer, default=0)
    number_commits = Column(Integer, default=0)
    number_organizations = Column(Integer, default=0)
    number_contributors = Column(Integer, default=0)
    
    __table_args__ = (
        # Composite unique constraint for year and country
        {'sqlite_autoincrement': True},
    )

def init_db(database_url: str, reset_db: bool = False) -> sessionmaker:
    """Initialize database.
    
    Args:
        database_url: Database URL
        reset_db: If True, reset the database by removing existing file (for SQLite)
    """
    # Wenn SQLite-Datenbank und reset_db ist True, prüfe ob die Datei existiert und lösche sie ggf.
    if reset_db and database_url.startswith('sqlite:///'):
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
    logger.info("Creating database tables if they don't exist...")
    Base.metadata.create_all(engine)
    logger.info("Database tables created successfully")
    
    # Erstelle und gib Session-Factory zurück
    return sessionmaker(bind=engine)