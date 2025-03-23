"""Tests for repository mapper."""

import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock

from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, scoped_session
from sqlalchemy.orm.session import sessionmaker

from src.github_database.mapping.repository_mapper import RepositoryMapper
from src.github_database.database.database import Base, Repository, User, Organization

@pytest.fixture
def db_session():
    """Create test database session."""
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    Session = scoped_session(sessionmaker(bind=engine))
    session = Session()
    yield session
    session.close()
    Base.metadata.drop_all(engine)

@pytest.fixture
def api_data():
    """Sample GitHub API repository data."""
    return {
        'id': 1,
        'name': 'test-repo',
        'full_name': 'testuser/test-repo',
        'owner': {
            'id': 1,
            'login': 'testuser',
            'type': 'User',
            'name': 'Test User',
            'email': 'test@example.com',
            'avatar_url': 'https://example.com/avatar.png',
            'company': 'Test Company',
            'blog': 'https://example.com',
            'location': 'Test Location',
            'bio': 'Test Bio',
            'public_repos': 10,
            'followers': 20,
            'following': 30,
            'created_at': '2020-01-01T00:00:00Z',
            'updated_at': '2020-01-01T00:00:00Z'
        },
        'description': 'Test repository',
        'homepage': 'https://example.com',
        'language': 'Python',
        'private': False,
        'fork': False,
        'default_branch': 'main',
        'size': 1000,
        'stargazers_count': 100,
        'watchers_count': 100,
        'forks_count': 50,
        'open_issues_count': 10,
        'created_at': '2020-01-01T00:00:00Z',
        'updated_at': '2020-01-01T00:00:00Z',
        'pushed_at': '2020-01-01T00:00:00Z'
    }

@pytest.fixture
def bigquery_data():
    """Sample BigQuery event data."""
    return {
        'repo': {
            'id': 1,
            'name': 'testuser/test-repo'
        },
        'actor': {
            'id': 1,
            'login': 'testuser'
        }
    }

def test_map_from_api_user_owner(db_session, api_data):
    """Test mapping repository with user owner from API data."""
    repo = RepositoryMapper.map_from_api(api_data, db_session)
    db_session.add(repo)
    db_session.commit()
    
    assert repo.id == 1
    assert repo.name == 'test-repo'
    assert repo.full_name == 'testuser/test-repo'
    assert repo.owner.login == 'testuser'
    assert repo.owner.type == 'User'
    assert repo.description == 'Test repository'
    assert repo.language == 'Python'
    assert repo.stargazers_count == 100

def test_map_from_api_org_owner(db_session, api_data):
    """Test mapping repository with organization owner from API data."""
    api_data['owner']['type'] = 'Organization'
    repo = RepositoryMapper.map_from_api(api_data, db_session)
    db_session.add(repo)
    db_session.commit()
    
    assert repo.id == 1
    assert repo.name == 'test-repo'
    assert repo.full_name == 'testuser/test-repo'
    assert repo.owner.login == 'testuser'
    assert isinstance(repo.owner, Organization)
    assert repo.description == 'Test repository'

def test_map_from_bigquery(bigquery_data):
    """Test mapping repository from BigQuery data."""
    data = RepositoryMapper.map_from_bigquery(bigquery_data)
    
    assert data['id'] == 1
    assert data['name'] == 'test-repo'
    assert data['full_name'] == 'testuser/test-repo'
    assert data['owner']['login'] == 'testuser'
