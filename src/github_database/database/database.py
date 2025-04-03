"""Database models for GitHub data."""

from datetime import datetime
from typing import Optional, List
import os
import logging

from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime,
    ForeignKey, Text, create_engine, Table, func, desc
)
from sqlalchemy.orm import relationship, declarative_base, sessionmaker

logger = logging.getLogger(__name__)

Base = declarative_base()

# Intermediate table for the relationship between Contributors and Repositories
contributor_repository = Table(
    'contributor_repository', Base.metadata,
    Column('contributor_id', Integer, ForeignKey('contributors.id'), primary_key=True),
    Column('repository_id', Integer, ForeignKey('repositories.id'), primary_key=True),
    Column('contributions', Integer, default=0),
    Column('first_contribution_at', DateTime),
    Column('last_contribution_at', DateTime)
)

# Intermediate table for the relationship between Contributors and Organizations
contributor_organization = Table(
    'contributor_organization', Base.metadata,
    Column('contributor_id', Integer, ForeignKey('contributors.id'), primary_key=True),
    Column('organization_id', Integer, ForeignKey('organizations.id'), primary_key=True),
    Column('joined_at', DateTime, default=datetime.utcnow)
)

class Contributor(Base):
    """Contributor model."""
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
    country_code = Column(String(2))  # ISO country code
    region = Column(String)  # Region within the country or continent
    bio = Column(Text)
    twitter_username = Column(String)
    public_repos = Column(Integer)
    public_gists = Column(Integer)
    followers = Column(Integer)
    following = Column(Integer)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)
    
    # Relationships
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
    country_code = Column(String(2))  # ISO country code
    region = Column(String)  # Region within the country or continent
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
    contributors = relationship('Contributor', secondary=contributor_organization, back_populates='organizations')

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
    
    # GitHub API stats
    stargazers_count = Column(Integer, default=0)
    watchers_count = Column(Integer, default=0)
    forks_count = Column(Integer, default=0)
    open_issues_count = Column(Integer, default=0)
    
    # Timestamps
    created_at = Column(DateTime)
    updated_at = Column(DateTime)
    pushed_at = Column(DateTime)
    
    # Relationships
    owner = relationship('Contributor', foreign_keys=[owner_id])
    organization = relationship('Organization', back_populates='repositories')
    contributors = relationship('Contributor', secondary=contributor_repository, back_populates='repositories')

def init_db(database_url: str, reset_db: bool = False) -> sessionmaker:
    """Initialize database.
    
    Args:
        database_url: Database URL
        reset_db: If True, reset the database by removing existing file (for SQLite)
    """
    # For SQLite databases, check if the file exists and delete it if reset_db is True
    if reset_db and database_url.startswith('sqlite:///'):
        db_path = database_url.replace('sqlite:///', '')
        if os.path.exists(db_path):
            try:
                logger.info(f"Removing existing database file: {db_path}")
                os.remove(db_path)
                logger.info("Existing database file removed successfully")
            except Exception as e:
                logger.error(f"Error removing database file: {e}")
    
    # Create engine and tables
    engine = create_engine(database_url)
    logger.info("Creating database tables if they don't exist...")
    Base.metadata.create_all(engine)
    
    # Return session factory
    return sessionmaker(bind=engine)

class GitHubDatabase:
    """
    Main interface for database operations.
    
    This class provides methods for inserting, querying, and updating
    GitHub data in the database.
    """
    
    def __init__(self, db_path: str = None):
        """
        Initialize database connection.
        
        Args:
            db_path: Path to SQLite database or None for in-memory DB
        """
        if db_path:
            # SQLite database path
            if not db_path.startswith('sqlite:///'):
                db_path = f'sqlite:///{db_path}'
        else:
            # In-memory database for tests
            db_path = 'sqlite:///:memory:'
            
        self.db_url = db_path
        self.Session = init_db(db_path)
        self.session = self.Session()
        logger.info(f"Database connection initialized: {db_path}")
    
    def close(self):
        """Close database connection."""
        if self.session:
            self.session.close()
            logger.info("Database connection closed")
    
    def insert_repository(self, repo_data: dict) -> Repository:
        """
        Insert a repository into the database.
        
        Args:
            repo_data: Repository data from GitHub API or ETL orchestrator
            
        Returns:
            Repository object
        """
        # Check if repository already exists
        existing = self.session.query(Repository).filter_by(id=repo_data['id']).first()
        if existing:
            logger.debug(f"Repository {repo_data['full_name']} already exists, updating instead")
            return self.update_repository(existing, repo_data)
        
        # Create new repository object
        repo = Repository()
        self._update_repo_fields(repo, repo_data)
        
        # Process owner information
        if 'owner_id' in repo_data and repo_data['owner_id']:
            # Direkt übergebene owner_id verwenden
            repo.owner_id = repo_data['owner_id']
        else:
            # Versuche, den Besitzer aus dem owner-Schlüssel zu extrahieren
            owner_data = repo_data.get('owner', {})
            if owner_data:
                owner = self.get_or_create_contributor(owner_data)
                repo.owner_id = owner.id
        
        # Process organization if it exists
        if 'organization_id' in repo_data and repo_data['organization_id']:
            # Direkt übergebene organization_id verwenden
            repo.organization_id = repo_data['organization_id']
        else:
            # Versuche, die Organisation aus dem organization-Schlüssel zu extrahieren
            org_data = repo_data.get('organization', {})
            if org_data:
                organization = self.get_or_create_organization(org_data)
                repo.organization_id = organization.id
        
        # Save to database
        self.session.add(repo)
        self.session.commit()
        logger.debug(f"Repository {repo.full_name} inserted into database")
        
        return repo
    
    def update_repository(self, repo: Repository, repo_data: dict) -> Repository:
        """
        Update an existing repository with new data.
        
        Args:
            repo: Repository object to update
            repo_data: New repository data
            
        Returns:
            Updated Repository object
        """
        self._update_repo_fields(repo, repo_data)
        self.session.commit()
        logger.debug(f"Repository {repo.full_name} updated")
        
        return repo
    
    def _update_repo_fields(self, repo: Repository, repo_data: dict):
        """
        Update repository fields from data dictionary.
        
        Args:
            repo: Repository object to update
            repo_data: Repository data dictionary
        """
        # Map data to repository fields
        field_mapping = {
            'id': 'id',
            'name': 'name',
            'full_name': 'full_name',
            'description': 'description',
            'homepage': 'homepage',
            'language': 'language',
            'private': 'private',
            'fork': 'fork',
            'default_branch': 'default_branch',
            'size': 'size',
            'stargazers_count': 'stargazers_count',
            'watchers_count': 'watchers_count',
            'forks_count': 'forks_count',
            'open_issues_count': 'open_issues_count',
        }
        
        # Update fields
        for api_field, db_field in field_mapping.items():
            if api_field in repo_data:
                setattr(repo, db_field, repo_data[api_field])
        
        # Convert datetime fields
        for dt_field in ['created_at', 'updated_at', 'pushed_at']:
            if dt_field in repo_data and repo_data[dt_field]:
                if isinstance(repo_data[dt_field], str):
                    # Convert string to datetime
                    try:
                        dt_value = datetime.fromisoformat(repo_data[dt_field].replace('Z', '+00:00'))
                        setattr(repo, dt_field, dt_value)
                    except ValueError:
                        logger.warning(f"Invalid datetime format for {dt_field}: {repo_data[dt_field]}")
                else:
                    # Use directly if already a datetime
                    setattr(repo, dt_field, repo_data[dt_field])
    
    def get_or_create_contributor(self, user_data: dict) -> Contributor:
        """
        Get an existing contributor or create a new one.
        
        Args:
            user_data: User data from GitHub API
            
        Returns:
            Contributor object
        """
        # Check if contributor already exists
        contributor = self.session.query(Contributor).filter_by(id=user_data['id']).first()
        
        if contributor:
            logger.debug(f"Contributor {user_data['login']} already exists")
            return contributor
        
        # Create new contributor
        contributor = Contributor()
        
        # Map user data to contributor fields
        field_mapping = {
            'id': 'id',
            'login': 'login',
            'name': 'name',
            'email': 'email',
            'type': 'type',
            'avatar_url': 'avatar_url',
            'company': 'company',
            'blog': 'blog',
            'location': 'location',
            'bio': 'bio',
            'twitter_username': 'twitter_username',
            'public_repos': 'public_repos',
            'public_gists': 'public_gists',
            'followers': 'followers',
            'following': 'following',
        }
        
        # Update fields
        for api_field, db_field in field_mapping.items():
            if api_field in user_data:
                setattr(contributor, db_field, user_data[api_field])
        
        # Convert datetime fields
        for dt_field in ['created_at', 'updated_at']:
            if dt_field in user_data and user_data[dt_field]:
                if isinstance(user_data[dt_field], str):
                    try:
                        dt_value = datetime.fromisoformat(user_data[dt_field].replace('Z', '+00:00'))
                        setattr(contributor, dt_field, dt_value)
                    except ValueError:
                        logger.warning(f"Invalid datetime format for {dt_field}: {user_data[dt_field]}")
                else:
                    setattr(contributor, dt_field, user_data[dt_field])
        
        # Save to database
        self.session.add(contributor)
        self.session.commit()
        logger.debug(f"Contributor {contributor.login} inserted into database")
        
        return contributor
    
    def get_or_create_organization(self, org_data: dict) -> Organization:
        """
        Get an existing organization or create a new one.
        
        Args:
            org_data: Organization data from GitHub API
            
        Returns:
            Organization object
        """
        # Check if organization already exists
        organization = self.session.query(Organization).filter_by(id=org_data['id']).first()
        
        if organization:
            logger.debug(f"Organization {org_data['login']} already exists")
            return organization
        
        # Create new organization
        organization = Organization()
        
        # Map organization data to organization fields
        field_mapping = {
            'id': 'id',
            'login': 'login',
            'name': 'name',
            'email': 'email',
            'type': 'type',
            'avatar_url': 'avatar_url',
            'company': 'company',
            'blog': 'blog',
            'location': 'location',
            'bio': 'bio',
            'twitter_username': 'twitter_username',
            'public_repos': 'public_repos',
            'public_gists': 'public_gists',
            'followers': 'followers',
            'following': 'following',
            'public_members': 'public_members',
        }
        
        # Update fields
        for api_field, db_field in field_mapping.items():
            if api_field in org_data:
                setattr(organization, db_field, org_data[api_field])
        
        # Convert datetime fields
        for dt_field in ['created_at', 'updated_at']:
            if dt_field in org_data and org_data[dt_field]:
                if isinstance(org_data[dt_field], str):
                    try:
                        dt_value = datetime.fromisoformat(org_data[dt_field].replace('Z', '+00:00'))
                        setattr(organization, dt_field, dt_value)
                    except ValueError:
                        logger.warning(f"Invalid datetime format for {dt_field}: {org_data[dt_field]}")
                else:
                    setattr(organization, dt_field, org_data[dt_field])
        
        # Save to database
        self.session.add(organization)
        self.session.commit()
        logger.debug(f"Organization {organization.login} inserted into database")
        
        return organization
    
    def add_contributor_to_repository(self, contributor: Contributor, repository: Repository, 
                                     contributions: int = None) -> None:
        """
        Add a contributor to a repository with contribution information.
        
        Args:
            contributor: Contributor object
            repository: Repository object
            contributions: Number of contributions
        """
        # Check if relationship already exists
        if contributor in repository.contributors:
            logger.debug(f"Contributor {contributor.login} already associated with repository {repository.full_name}")
            return
        
        # Add contributor to repository
        repository.contributors.append(contributor)
        
        # Update contribution count if provided
        if contributions is not None:
            # Get the association object from the session
            stmt = contributor_repository.update().where(
                (contributor_repository.c.contributor_id == contributor.id) &
                (contributor_repository.c.repository_id == repository.id)
            ).values(contributions=contributions)
            
            self.session.execute(stmt)
        
        self.session.commit()
        logger.debug(f"Added contributor {contributor.login} to repository {repository.full_name}")
    
    def add_contributor_to_organization(self, contributor: Contributor, organization: Organization) -> None:
        """
        Add a contributor to an organization.
        
        Args:
            contributor: Contributor object
            organization: Organization object
        """
        # Check if relationship already exists
        if contributor in organization.contributors:
            logger.debug(f"Contributor {contributor.login} already associated with organization {organization.login}")
            return
        
        # Add contributor to organization
        organization.contributors.append(contributor)
        self.session.commit()
        logger.debug(f"Added contributor {contributor.login} to organization {organization.login}")
    
    def get_repository_by_id(self, repo_id: int) -> Optional[Repository]:
        """
        Get a repository by its ID.
        
        Args:
            repo_id: Repository ID
            
        Returns:
            Repository object or None if not found
        """
        return self.session.query(Repository).filter_by(id=repo_id).first()
    
    def get_repository_by_name(self, full_name: str) -> Optional[Repository]:
        """
        Get a repository by its full name.
        
        Args:
            full_name: Repository full name (owner/name)
            
        Returns:
            Repository object or None if not found
        """
        return self.session.query(Repository).filter_by(full_name=full_name).first()
    
    def get_contributor_by_id(self, contributor_id: int) -> Optional[Contributor]:
        """
        Get a contributor by ID.
        
        Args:
            contributor_id: Contributor ID
            
        Returns:
            Contributor object or None if not found
        """
        return self.session.query(Contributor).filter_by(id=contributor_id).first()
    
    def get_contributor_by_login(self, login: str) -> Optional[Contributor]:
        """
        Get contributor by login.
        
        Args:
            login: Contributor's GitHub login
            
        Returns:
            Contributor object or None if not found
        """
        return self.session.query(Contributor).filter_by(login=login).first()
    
    def get_organization_by_id(self, org_id: int) -> Optional[Organization]:
        """
        Get an organization by ID.
        
        Args:
            org_id: Organization ID
            
        Returns:
            Organization object or None if not found
        """
        return self.session.query(Organization).filter_by(id=org_id).first()
    
    def get_organization_by_login(self, login: str) -> Optional[Organization]:
        """
        Get organization by login.
        
        Args:
            login: Organization's GitHub login
            
        Returns:
            Organization object or None if not found
        """
        return self.session.query(Organization).filter_by(login=login).first()
    
    def get_repository_count(self) -> int:
        """
        Get the total number of repositories in the database.
        
        Returns:
            Number of repositories
        """
        return self.session.query(Repository).count()
    
    def get_contributor_count(self) -> int:
        """
        Get the total number of contributors in the database.
        
        Returns:
            Number of contributors
        """
        return self.session.query(Contributor).count()
    
    def get_organization_count(self) -> int:
        """
        Get the total number of organizations in the database.
        
        Returns:
            Number of organizations
        """
        return self.session.query(Organization).count()
    
    def get_repository_by_owner_and_name(self, owner: str, name: str) -> Optional[Repository]:
        """
        Get repository by owner login and repository name.
        
        Args:
            owner: Owner login (user or organization)
            name: Repository name
            
        Returns:
            Repository object or None if not found
        """
        full_name = f"{owner}/{name}"
        repo = self.session.query(Repository).filter_by(full_name=full_name).first()
        return repo
    
    def get_language_statistics(self):
        """
        Get statistics about repository languages.
        
        Returns:
            List of tuples (language, count) sorted by count in descending order
        """
        try:
            from sqlalchemy import func, desc
            
            # Query for language statistics
            result = self.session.query(
                Repository.language, 
                func.count(Repository.id).label('count')
            ).filter(
                Repository.language.is_not(None),
                Repository.language != ''
            ).group_by(
                Repository.language
            ).order_by(
                desc('count')
            ).all()
            
            return result
        except Exception as e:
            logger.error(f"Error getting language statistics: {e}")
            return []
    
    def get_repository_date_range(self):
        """
        Get the date range of repositories in the database.
        
        Returns:
            Tuple of (earliest_date, latest_date) or None if no repositories
        """
        try:
            earliest = self.session.query(func.min(Repository.created_at)).scalar()
            latest = self.session.query(func.max(Repository.created_at)).scalar()
            
            if earliest and latest:
                return (earliest.strftime("%Y-%m-%d"), latest.strftime("%Y-%m-%d"))
            return None
        except Exception as e:
            logger.error(f"Error getting repository date range: {e}")
            return None
            
    def get_contributor_location_stats(self):
        """
        Get statistics about contributor locations.
        
        Returns:
            Dictionary with location statistics:
            - total: Total number of contributors
            - with_location: Number of contributors with location
            - with_country_code: Number of contributors with country code
            - location_percentage: Percentage of contributors with location
            - country_code_percentage: Percentage of contributors with country code
        """
        try:
            total = self.get_contributor_count()
            
            # Count contributors with location
            with_location = self.session.query(func.count(Contributor.id))\
                .filter(Contributor.location.isnot(None))\
                .filter(Contributor.location != '')\
                .scalar() or 0
                
            # Count contributors with country code
            with_country_code = self.session.query(func.count(Contributor.id))\
                .filter(Contributor.country_code.isnot(None))\
                .filter(Contributor.country_code != '')\
                .scalar() or 0
                
            # Calculate percentages
            location_percentage = (with_location / total * 100) if total > 0 else 0
            country_code_percentage = (with_country_code / total * 100) if total > 0 else 0
            country_code_from_location_percentage = (with_country_code / with_location * 100) if with_location > 0 else 0
            
            return {
                'total': total,
                'with_location': with_location,
                'with_country_code': with_country_code,
                'location_percentage': location_percentage,
                'country_code_percentage': country_code_percentage,
                'country_code_from_location_percentage': country_code_from_location_percentage
            }
        except Exception as e:
            logger.error(f"Error getting contributor location stats: {e}")
            return {
                'total': 0,
                'with_location': 0,
                'with_country_code': 0,
                'location_percentage': 0,
                'country_code_percentage': 0,
                'country_code_from_location_percentage': 0
            }
            
    def get_organization_location_stats(self):
        """
        Get statistics about organization locations.
        
        Returns:
            Dictionary with location statistics:
            - total: Total number of organizations
            - with_location: Number of organizations with location
            - with_country_code: Number of organizations with country code
            - location_percentage: Percentage of organizations with location
            - country_code_percentage: Percentage of organizations with country code
        """
        try:
            total = self.get_organization_count()
            
            # Count organizations with location
            with_location = self.session.query(func.count(Organization.id))\
                .filter(Organization.location.isnot(None))\
                .filter(Organization.location != '')\
                .scalar() or 0
                
            # Count organizations with country code
            with_country_code = self.session.query(func.count(Organization.id))\
                .filter(Organization.country_code.isnot(None))\
                .filter(Organization.country_code != '')\
                .scalar() or 0
                
            # Calculate percentages
            location_percentage = (with_location / total * 100) if total > 0 else 0
            country_code_percentage = (with_country_code / total * 100) if total > 0 else 0
            country_code_from_location_percentage = (with_country_code / with_location * 100) if with_location > 0 else 0
            
            return {
                'total': total,
                'with_location': with_location,
                'with_country_code': with_country_code,
                'location_percentage': location_percentage,
                'country_code_percentage': country_code_percentage,
                'country_code_from_location_percentage': country_code_from_location_percentage
            }
        except Exception as e:
            logger.error(f"Error getting organization location stats: {e}")
            return {
                'total': 0,
                'with_location': 0,
                'with_country_code': 0,
                'location_percentage': 0,
                'country_code_percentage': 0,
                'country_code_from_location_percentage': 0
            }