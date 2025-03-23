"""Tests for database models."""

import pytest
from datetime import datetime, timezone
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from src.github_database.database.database import (
    Base,
    User,
    Organization,
    Repository,
    Event,
    init_db
)

@pytest.fixture
def engine():
    """Create test database engine."""
    return create_engine('sqlite:///:memory:')

@pytest.fixture
def session(engine):
    """Create test database session."""
    Base.metadata.create_all(engine)
    Session = init_db('sqlite:///:memory:')
    session = Session()
    yield session
    session.close()
    Base.metadata.drop_all(engine)

def test_user_model(session):
    """Test User model."""
    user = User(
        id=1,
        login='testuser',
        name='Test User',
        email='test@example.com',
        type='User',
        created_at=datetime.now(timezone.utc)
    )
    session.add(user)
    session.commit()
    
    saved_user = session.query(User).first()
    assert saved_user.id == 1
    assert saved_user.login == 'testuser'
    assert saved_user.type == 'User'

def test_organization_model(session):
    """Test Organization model."""
    org = Organization(
        id=1,
        login='testorg',
        name='Test Org',
        email='org@example.com',
        created_at=datetime.now(timezone.utc)
    )
    session.add(org)
    session.commit()
    
    saved_org = session.query(Organization).first()
    assert saved_org.id == 1
    assert saved_org.login == 'testorg'

def test_repository_model(session):
    """Test Repository model."""
    # Create user
    user = User(
        id=1,
        login='testuser',
        type='User'
    )
    session.add(user)
    
    # Create repository
    repo = Repository(
        id=1,
        name='test-repo',
        full_name='testuser/test-repo',
        owner=user,
        description='Test repository',
        language='Python',
        created_at=datetime.now(timezone.utc)
    )
    session.add(repo)
    session.commit()
    
    saved_repo = session.query(Repository).first()
    assert saved_repo.id == 1
    assert saved_repo.name == 'test-repo'
    assert saved_repo.owner.login == 'testuser'

def test_event_model(session):
    """Test Event model."""
    # Create user and repository
    user = User(
        id=1,
        login='testuser',
        type='User'
    )
    session.add(user)
    
    repo = Repository(
        id=1,
        name='test-repo',
        full_name='testuser/test-repo',
        owner=user
    )
    session.add(repo)
    
    # Create event
    event = Event(
        id='123',
        type='PushEvent',
        actor=user,
        repository=repo,
        created_at=datetime.now(timezone.utc)
    )
    session.add(event)
    session.commit()
    
    saved_event = session.query(Event).first()
    assert saved_event.id == '123'
    assert saved_event.type == 'PushEvent'
    assert saved_event.actor.login == 'testuser'
    assert saved_event.repository.name == 'test-repo'

def test_relationships(session):
    """Test model relationships."""
    # Create organization
    org = Organization(
        id=1,
        login='testorg',
        name='Test Org'
    )
    session.add(org)
    
    # Create user
    user = User(
        id=1,
        login='testuser',
        type='User'
    )
    session.add(user)
    
    # Add user to organization
    org.members.append(user)
    
    # Create repository in organization
    repo = Repository(
        id=1,
        name='test-repo',
        full_name='testorg/test-repo',
        owner=user,
        organization=org
    )
    session.add(repo)
    
    # Create event
    event = Event(
        id='123',
        type='PushEvent',
        actor=user,
        repository=repo,
        created_at=datetime.now(timezone.utc)
    )
    session.add(event)
    session.commit()
    
    # Test relationships
    assert user in org.members
    assert repo in org.repositories
    assert repo.owner == user
    assert repo.organization == org
    assert event in repo.events
