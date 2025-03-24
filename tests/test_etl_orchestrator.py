"""Tests for ETL orchestrator."""

import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone
import time

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
            'name': 'Test User',
            'location': 'San Francisco, CA',
            'created_at': '2020-01-01T00:00:00Z',
            'updated_at': '2023-01-01T00:00:00Z',
            'type': 'User'
        }
        client.get_organization.return_value = {
            'id': 1,
            'login': 'testorg',
            'name': 'Test Org',
            'location': 'Berlin, Germany',
            'created_at': '2020-01-01T00:00:00Z',
            'updated_at': '2023-01-01T00:00:00Z'
        }
        client.get_repository.return_value = {
            'id': 1,
            'name': 'testrepo',
            'full_name': 'testorg/testrepo',
            'owner': {
                'id': 1,
                'login': 'testorg',
                'type': 'Organization'
            },
            'organization': {
                'id': 1,
                'login': 'testorg'
            },
            'stargazers_count': 100,
            'forks_count': 50,
            'created_at': '2020-01-01T00:00:00Z',
            'updated_at': '2023-01-01T00:00:00Z',
            'pushed_at': '2023-01-01T00:00:00Z'
        }
        client.get_repository_contributors.return_value = [{
            'id': 2,
            'login': 'contributor',
            'location': 'New York, NY',
            'type': 'User',
            'created_at': '2020-01-01T00:00:00Z',
            'updated_at': '2023-01-01T00:00:00Z'
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
        return client
        
    @pytest.fixture
    def etl(self, db_session, mock_github_client, mock_bigquery_client):
        """Create ETL orchestrator with mocked clients."""
        config = ETLConfig(
            database_url='sqlite:///:memory:',
            github=MagicMock(),
            bigquery=MagicMock()
        )
        # Setze die Qualitätsfilter
        config.min_stars = 10
        config.min_forks = 5
        config.min_commits_last_year = 50
        config.batch_size = 100
        
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
        
    def test_get_or_create_contributor(self, etl):
        """Test contributor creation and retrieval."""
        # Test creation
        contributor = etl._get_or_create_contributor({
            'id': 1,
            'login': 'testuser',
            'location': 'San Francisco, CA'
        }, etl.session)
        
        assert contributor.id == 1
        assert contributor.login == 'testuser'
        assert contributor.country_code is not None
        
        # Test retrieval of existing contributor
        same_contributor = etl._get_or_create_contributor({
            'id': 1,
            'login': 'testuser'
        }, etl.session)
        
        assert same_contributor.id == 1
        assert same_contributor is contributor
        
    def test_get_or_create_organization(self, etl):
        """Test organization creation and retrieval."""
        # Test creation
        org = etl._get_or_create_organization({
            'id': 1,
            'login': 'testorg',
            'location': 'Berlin, Germany'
        }, etl.session)
        
        assert org.id == 1
        assert org.login == 'testorg'
        assert org.country_code is not None
        
        # Test retrieval of existing organization
        same_org = etl._get_or_create_organization({
            'id': 1,
            'login': 'testorg'
        }, etl.session)
        
        assert same_org.id == 1
        assert same_org is org
        
    def test_process_repository(self, etl, db_session):
        """Test repository processing."""
        # Process repository
        repo = etl.process_repository('testorg/testrepo', db_session)
        
        # Verify repository was created
        assert repo.id == 1
        assert repo.name == 'testrepo'
        assert repo.full_name == 'testorg/testrepo'
        assert repo.stars == 100
        assert repo.forks == 50
        
        # Verify organization was created
        assert repo.organization.id == 1
        assert repo.organization.login == 'testorg'
        
        # Verify contributors were added
        assert len(repo.contributors) == 1
        assert repo.contributors[0].login == 'testuser'
        
    def test_get_quality_repositories(self, etl):
        """Test quality repository retrieval."""
        # Mock GitHub search response
        etl.github_client.search_repositories.return_value = [
            {'full_name': 'org1/repo1', 'stargazers_count': 100, 'forks_count': 50},
            {'full_name': 'org2/repo2', 'stargazers_count': 200, 'forks_count': 100}
        ]
        
        # Get quality repositories
        repos = etl.get_quality_repositories(limit=10)
        
        # Verify repositories were returned
        assert len(repos) == 2
        assert repos[0]['full_name'] == 'org1/repo1'
        assert repos[1]['full_name'] == 'org2/repo2'
        
    def test_extract_location_data(self, etl):
        """Test location data extraction."""
        # Test mit US-Standort
        country_code, region = etl._extract_location_data('San Francisco, CA')
        assert country_code == 'US'
        assert region == 'North America'
        
        # Test mit deutschem Standort
        country_code, region = etl._extract_location_data('Berlin, Germany')
        assert country_code == 'DE'
        assert region == 'Europe'
        
        # Test mit unbekanntem Standort
        country_code, region = etl._extract_location_data('Unknown Location')
        assert country_code is None
        assert region is None
        
        # Warte kurz, damit asynchrone Geocoding-Aufgaben abgeschlossen werden können
        import time
        time.sleep(2)
        
        # Überprüfe, ob der Cache aktualisiert wurde
        assert 'San Francisco, CA' in etl.geocoding_cache
        assert 'Berlin, Germany' in etl.geocoding_cache
        
    def test_rate_limit_handling(self, etl, db_session):
        """Test handling of rate limit errors."""
        # Mock GitHub client to raise rate limit error
        reset_time = time.time() + 3600  # Reset in einer Stunde
        etl.github_client.get_repository.side_effect = RateLimitError('Rate limit exceeded', reset_time)
        
        # Try to process repository
        repo = etl.process_repository('testorg/testrepo', db_session)
        
        # Verify repository was not created
        assert repo is None

    def test_api_error_handling(self, etl, db_session):
        """Test handling of general API errors."""
        # Mock GitHub client to raise API error
        etl.github_client.get_repository.side_effect = GitHubAPIError('API error')
        
        # Process repository should handle the error gracefully
        repo = etl.process_repository('testorg/testrepo', db_session)
        
        # Repository should not be created
        assert repo is None

    def test_async_geocoding(self, etl):
        """Test die asynchrone Geocoding-Funktionalität."""
        # Leere den Cache für diesen Test
        etl.geocoding_cache = {}
        
        # Rufe die Methode auf, die asynchrones Geocoding verwendet
        country_code, region = etl._extract_location_data('Berlin, Germany')
        
        # Die erste Anfrage sollte die Heuristik verwenden und sofort zurückkehren
        assert country_code == 'DE'  # Die Heuristik sollte DE erkennen
        
        # Warte, bis die asynchrone Verarbeitung abgeschlossen ist
        import time
        time.sleep(2)
        
        # Überprüfe, ob der Cache aktualisiert wurde
        assert 'Berlin, Germany' in etl.geocoding_cache
        
        # Überprüfe, ob ein zweiter Aufruf den Cache verwendet
        with patch.object(etl, '_geocode_location') as mock_geocode:
            country_code, region = etl._extract_location_data('Berlin, Germany')
            mock_geocode.assert_not_called()  # Sollte nicht aufgerufen werden, da im Cache
            
        # Teste mit einem unbekannten Standort
        country_code, region = etl._extract_location_data('Unknown Location XYZ')
        assert country_code is None
        assert region is None
        
        # Warte auf asynchrone Verarbeitung
        time.sleep(2)
        
        # Überprüfe, ob der unbekannte Standort im Cache ist
        assert 'Unknown Location XYZ' in etl.geocoding_cache
