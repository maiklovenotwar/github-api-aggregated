"""Tests for ETL orchestrator."""

import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.github_database.config import ETLConfig
from src.github_database.database.database import Base
from src.github_database.etl_orchestrator import ETLOrchestrator
from src.github_database.api.github_api import RateLimitError, GitHubAPIError

class TestETLOrchestrator:
    """Test cases for ETLOrchestrator class."""
    
    @pytest.fixture
    def db_session(self):
        """Create test database session."""
        engine = create_engine('sqlite:///:memory:')
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        session = Session()
        yield session
        session.close()
        Base.metadata.drop_all(engine)
        
    @pytest.fixture
    def mock_github_client(self):
        """Create mock GitHub client."""
        client = MagicMock()
        client.get_user.return_value = {
            'id': 1,
            'login': 'testuser',
            'name': 'Test User'
        }
        client.get_organization.return_value = {
            'id': 1,
            'login': 'testorg',
            'name': 'Test Org'
        }
        client.get_repository.return_value = {
            'id': 1,
            'name': 'testrepo',
            'full_name': 'testuser/testrepo',
            'owner': {
                'id': 1,
                'login': 'testuser',
                'type': 'User'
            }
        }
        client.get_repository_contributors.return_value = [{
            'id': 2,
            'login': 'contributor'
        }]
        return client
        
    @pytest.fixture
    def mock_bigquery_client(self):
        """Create mock BigQuery client."""
        client = MagicMock()
        client.get_repository_metrics.return_value = {
            'push_count': 100,
            'branch_count': 5,
            'commit_count': 150,
            'total_commits': 300
        }
        client.get_events.return_value = [{
            'id': '1',
            'type': 'PushEvent',
            'actor': {'id': 1, 'login': 'testuser'},
            'repo': {'id': 1, 'name': 'testuser/testrepo'},
            'created_at': datetime(2025, 3, 22, tzinfo=timezone.utc),
            'payload': {'ref': 'refs/heads/main'}
        }]
        return client
        
    @pytest.fixture
    def etl(self, db_session, mock_github_client, mock_bigquery_client):
        """Create ETL orchestrator with mocked clients."""
        config = ETLConfig()
        orchestrator = ETLOrchestrator(config)
        orchestrator.session = db_session
        orchestrator.github_client = mock_github_client
        orchestrator.bigquery_client = mock_bigquery_client
        return orchestrator
        
    def test_initialization(self, etl):
        """Test proper initialization."""
        assert etl.config is not None
        assert etl.session is not None
        assert etl.github_client is not None
        assert etl.bigquery_client is not None
        
    def test_get_or_create_user(self, etl):
        """Test user creation and retrieval."""
        # Test creation
        user = etl._get_or_create_user({
            'id': 1,
            'login': 'testuser'
        })
        assert user.id == 1
        assert user.login == 'testuser'
        
        # Test retrieval of existing user
        same_user = etl._get_or_create_user({
            'id': 1,
            'login': 'testuser'
        })
        assert same_user.id == user.id
        
    def test_get_or_create_organization(self, etl):
        """Test organization creation and retrieval."""
        # Test creation
        org = etl._get_or_create_organization({
            'id': 1,
            'login': 'testorg'
        })
        assert org.id == 1
        assert org.login == 'testorg'
        
        # Test retrieval of existing organization
        same_org = etl._get_or_create_organization({
            'id': 1,
            'login': 'testorg'
        })
        assert same_org.id == org.id
        
    def test_get_or_create_repository(self, etl):
        """Test repository creation and retrieval."""
        # Test creation
        repo = etl._get_or_create_repository({
            'id': 1,
            'full_name': 'testuser/testrepo'
        })
        assert repo.id == 1
        assert repo.full_name == 'testuser/testrepo'
        assert repo.owner.login == 'testuser'
        assert len(repo.contributors) == 1
        
        # Test retrieval of existing repository
        same_repo = etl._get_or_create_repository({
            'id': 1,
            'full_name': 'testuser/testrepo'
        })
        assert same_repo.id == repo.id
        
    def test_process_repository(self, etl):
        """Test repository processing."""
        repo = etl.process_repository('testuser/testrepo')
        assert repo is not None
        assert repo.full_name == 'testuser/testrepo'
        assert repo.owner.login == 'testuser'
        assert len(repo.contributors) == 1
        
    def test_update_yearly_data(self, etl):
        """Test yearly data update."""
        etl.update_yearly_data(2025)
        
        # Check that event was created
        events = etl.session.query(Event).all()
        assert len(events) == 1
        assert events[0].type == 'PushEvent'
        assert events[0].actor.login == 'testuser'
        assert events[0].repository.full_name == 'testuser/testrepo'
        
    def test_handle_api_error(self, etl):
        """Test API error handling."""
        # Test rate limit error
        error = RateLimitError(
            "Rate limit exceeded",
            datetime.now(timezone.utc).timestamp() + 3600
        )
        with pytest.raises(RateLimitError):
            etl._handle_api_error(error, "test")
            
        # Test 404 error
        error = GitHubAPIError("Not found", 404)
        etl._handle_api_error(error, "test")  # Should not raise
        
        # Test other error
        error = GitHubAPIError("Server error", 500)
        with pytest.raises(GitHubAPIError):
            etl._handle_api_error(error, "test")
