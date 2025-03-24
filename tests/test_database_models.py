"""Tests for database models."""

import pytest
from datetime import datetime, timezone
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from src.github_database.database.database import (
    Base,
    Contributor,
    Organization,
    Repository,
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
    Session = init_db('sqlite:///:memory:', reset_db=True)
    session = Session()
    yield session
    session.close()
    Base.metadata.drop_all(engine)

def test_contributor_model(session):
    """Test Contributor model."""
    contributor = Contributor(
        id=1,
        login='testuser',
        name='Test User',
        email='test@example.com',
        type='User',
        location='San Francisco, USA',
        country_code='US',
        region='North America',
        created_at=datetime.now(timezone.utc)
    )
    session.add(contributor)
    session.commit()
    
    saved_contributor = session.query(Contributor).first()
    assert saved_contributor.id == 1
    assert saved_contributor.login == 'testuser'
    assert saved_contributor.type == 'User'
    assert saved_contributor.country_code == 'US'
    assert saved_contributor.region == 'North America'

def test_organization_model(session):
    """Test Organization model."""
    org = Organization(
        id=1,
        login='testorg',
        name='Test Org',
        email='org@example.com',
        location='Berlin, Germany',
        country_code='DE',
        region='Europe',
        created_at=datetime.now(timezone.utc)
    )
    session.add(org)
    session.commit()
    
    saved_org = session.query(Organization).first()
    assert saved_org.id == 1
    assert saved_org.login == 'testorg'
    assert saved_org.country_code == 'DE'
    assert saved_org.region == 'Europe'

def test_repository_model(session):
    """Test Repository model."""
    # Create contributor
    contributor = Contributor(
        id=1,
        login='testuser',
        type='User'
    )
    session.add(contributor)
    
    # Create organization
    org = Organization(
        id=1,
        login='testorg',
        name='Test Org'
    )
    session.add(org)
    
    # Create repository
    repo = Repository(
        id=1,
        name='test-repo',
        full_name='testorg/test-repo',
        description='Test repository',
        language='Python',
        stargazers_count=100,
        forks_count=50,
        created_at=datetime.now(timezone.utc),
        organization_id=1,
        owner_id=1
    )
    session.add(repo)
    
    # Add contributor to repository
    repo.contributors.append(contributor)
    
    session.commit()
    
    saved_repo = session.query(Repository).first()
    assert saved_repo.id == 1
    assert saved_repo.name == 'test-repo'
    assert saved_repo.organization.login == 'testorg'
    assert len(saved_repo.contributors) == 1
    assert saved_repo.contributors[0].login == 'testuser'

def test_relationships(session):
    """Test model relationships."""
    # Create contributors
    contributor1 = Contributor(id=1, login='user1', type='User')
    contributor2 = Contributor(id=2, login='user2', type='User')
    session.add_all([contributor1, contributor2])
    
    # Create organizations
    org1 = Organization(id=1, login='org1')
    org2 = Organization(id=2, login='org2')
    session.add_all([org1, org2])
    
    # Create repositories
    repo1 = Repository(
        id=1,
        name='repo1',
        full_name='org1/repo1',
        organization_id=1,
        owner_id=1
    )
    repo2 = Repository(
        id=2,
        name='repo2',
        full_name='org2/repo2',
        organization_id=2,
        owner_id=1
    )
    session.add_all([repo1, repo2])
    
    # Add contributors to repositories
    repo1.contributors.append(contributor1)
    repo1.contributors.append(contributor2)
    repo2.contributors.append(contributor1)
    
    session.commit()
    
    # Test organization-repository relationship
    assert len(org1.repositories) == 1
    assert org1.repositories[0].name == 'repo1'
    assert len(org2.repositories) == 1
    assert org2.repositories[0].name == 'repo2'
    
    # Test repository-contributor relationship
    assert len(repo1.contributors) == 2
    assert repo1.contributors[0].login == 'user1'
    assert repo1.contributors[1].login == 'user2'
    assert len(repo2.contributors) == 1
    assert repo2.contributors[0].login == 'user1'
    
    # Test contributor-repository relationship
    assert len(contributor1.repositories) == 2
    assert contributor1.repositories[0].name == 'repo1'
    assert contributor1.repositories[1].name == 'repo2'
    assert len(contributor2.repositories) == 1
    assert contributor2.repositories[0].name == 'repo1'
