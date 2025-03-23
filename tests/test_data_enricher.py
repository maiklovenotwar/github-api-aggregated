"""Test data enrichment functionality."""

import unittest
from datetime import datetime, timezone
from pathlib import Path
import tempfile
from unittest.mock import MagicMock, patch, Mock
import time

from github_database.config import ETLConfig, GitHubConfig, BigQueryConfig
from github_database.config.etl_config import QualityConfig
from github_database.enrichment.data_enricher import DataEnricher, Cache, CacheConfig
from github_database.api.github_api import GitHubAPI

class TestCache(unittest.TestCase):
    """Test caching functionality."""
    
    def setUp(self):
        """Set up test environment."""
        self.cache = Cache(CacheConfig(), Path("test_cache"))
        
    def test_cache_set_get(self):
        """Test basic cache set/get operations."""
        self.cache.set('key1', 'value1')
        self.assertEqual(self.cache.get('key1'), 'value1')
        self.assertIsNone(self.cache.get('nonexistent'))
        
    def test_cache_invalidation(self):
        """Test cache invalidation."""
        self.cache.set('key1', 'value1')
        self.cache.invalidate('key1')
        self.assertIsNone(self.cache.get('key1'))
        
    def test_cache_clear(self):
        """Test cache clearing."""
        self.cache.set('key1', 'value1')
        self.cache.set('key2', 'value2')
        self.cache.clear()
        self.assertIsNone(self.cache.get('key1'))
        self.assertIsNone(self.cache.get('key2'))
        
    def test_memory_cache_eviction(self):
        """Test memory cache size limit and eviction."""
        # Create cache with small memory size
        cache = Cache(CacheConfig(memory_cache_size=2), Path("test_cache"))
        
        # Add items to fill cache
        cache.set('key1', 'value1')
        cache.set('key2', 'value2')
        
        # Access key1 to increase its access count
        cache.get('key1')
        
        # Add another item to trigger eviction
        cache.set('key3', 'value3')
        
        # key2 should be evicted (least accessed)
        self.assertIsNone(cache.get('key2'))
        
    def tearDown(self):
        """Clean up test environment."""
        self.cache.clear()
        cache_dir = Path("test_cache")
        if cache_dir.exists():
            for file in cache_dir.glob("*"):
                file.unlink()
            cache_dir.rmdir()

class TestDataEnricher(unittest.TestCase):
    """Test data enrichment functionality."""
    
    def setUp(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.config = ETLConfig(
            github=GitHubConfig(
                access_token="test_token",
                rate_limit_delay=0.1,
                parallel_requests=2
            ),
            bigquery=BigQueryConfig(project_id="test-project"),
            quality=QualityConfig()
        )
        self.enricher = DataEnricher(self.config, Path(self.temp_dir))
        
        # Mock GitHub API
        self.mock_github_api = MagicMock()
        self.enricher.github_api = self.mock_github_api
        
    def test_enrich_repository(self):
        """Test repository enrichment."""
        repo_dict = {'id': 1, 'name': 'test/repo'}
        api_response = {'description': 'test repo', 'language': 'Python'}
        
        self.mock_github_api.get_repository.return_value = api_response
        
        enriched_data = self.enricher.enrich_repository(repo_dict)
        
        self.mock_github_api.get_repository.assert_called_once_with('test', 'repo')
        self.assertEqual(enriched_data['description'], 'test repo')
        
    def test_enrich_user(self):
        """Test user enrichment."""
        user_dict = {'id': 1, 'login': 'testuser'}
        api_response = {'name': 'Test User', 'email': 'test@example.com'}
        
        self.mock_github_api.get_user.return_value = api_response
        
        enriched_data = self.enricher.enrich_user(user_dict)
        
        self.mock_github_api.get_user.assert_called_once_with('testuser')
        self.assertEqual(enriched_data['name'], 'Test User')
        
    def test_enrich_commit(self):
        """Test commit enrichment."""
        commit_dict = {'sha': 'abc123', 'repo': 'test/repo'}
        api_response = {'message': 'test commit', 'author': {'name': 'Test Author'}}
        
        self.mock_github_api.get_commit.return_value = api_response
        
        enriched_data = self.enricher.enrich_commit(commit_dict, 'test/repo')
        
        self.mock_github_api.get_commit.assert_called_once_with('test/repo', 'abc123')
        self.assertEqual(enriched_data['message'], 'test commit')
        
    def test_enrich_pull_request(self):
        """Test pull request enrichment."""
        pr_dict = {'number': 1, 'repo': 'test/repo'}
        api_response = {'title': 'test PR', 'state': 'open'}
        
        self.mock_github_api.get_pull_request.return_value = api_response
        
        enriched_data = self.enricher.enrich_pull_request(pr_dict, 'test/repo')
        
        self.mock_github_api.get_pull_request.assert_called_once_with('test/repo', 1)
        self.assertEqual(enriched_data['title'], 'test PR')
        
    def test_enrich_issue(self):
        """Test issue enrichment."""
        issue_dict = {'number': 1, 'repo': 'test/repo'}
        api_response = {'title': 'test issue', 'state': 'open'}
        
        self.mock_github_api.get_issue.return_value = api_response
        
        enriched_data = self.enricher.enrich_issue(issue_dict, 'test/repo')
        
        self.mock_github_api.get_issue.assert_called_once_with('test/repo', 1)
        self.assertEqual(enriched_data['title'], 'test issue')
        
    def test_batch_enrich_events(self):
        """Test batch event enrichment."""
        events = [
            {'type': 'PushEvent', 'repo': {'name': 'test/repo'}, 'payload': {'commits': [{'sha': 'abc123'}]}},
            {'type': 'IssuesEvent', 'repo': {'name': 'test/repo'}, 'payload': {'issue': {'number': 1}}}
        ]
        
        self.mock_github_api.get_commit.return_value = {'message': 'test commit'}
        self.mock_github_api.get_issue.return_value = {'title': 'test issue'}
        
        enriched_events = self.enricher.batch_enrich_events(events)
        
        self.assertEqual(len(enriched_events), 2)
        self.assertEqual(enriched_events[0]['payload']['commits'][0]['message'], 'test commit')
        self.assertEqual(enriched_events[1]['payload']['issue']['title'], 'test issue')
        
    def test_rate_limiting(self):
        """Test API rate limiting."""
        start_time = time.time()
        
        # Make multiple API calls
        for _ in range(3):
            self.enricher.enrich_repository({'id': 1, 'name': 'test/repo'})
            
        duration = time.time() - start_time
        
        # Should take at least 0.2 seconds due to rate limiting
        self.assertGreaterEqual(duration, 0.2)
        
    def tearDown(self):
        """Clean up test environment."""
        if hasattr(self, 'temp_dir'):
            import shutil
            shutil.rmtree(self.temp_dir)

class TestDataEnricherOrganization(unittest.TestCase):
    """Test data enrichment functionality for organization."""
    
    def setUp(self):
        """Set up test environment."""
        self.config = ETLConfig(
            github=GitHubConfig(access_token="test_token"),
            bigquery=BigQueryConfig(project_id="test-project"),
            quality=QualityConfig()
        )
        self.enricher = DataEnricher(self.config, Path("test_cache"))
        
        # Mock GitHub API
        self.mock_github_api = MagicMock()
        self.enricher.github_api = self.mock_github_api
        
    def test_enrich_organization(self):
        """Test organization enrichment."""
        # Test data
        org_dict = {
            'id': 1,
            'login': 'test-org'
        }
        
        api_response = {
            'name': 'Test Organization',
            'description': 'A test organization',
            'blog': 'https://test-org.com',
            'location': 'Test City',
            'email': 'test@test-org.com',
            'twitter_username': 'testorg',
            'public_repos': 100,
            'followers': 50,
            'following': 10,
            'created_at': '2020-01-01T00:00:00Z',
            'updated_at': '2020-01-02T00:00:00Z'
        }
        
        # Mock API response
        self.mock_github_api.get_organization.return_value = api_response
        
        # Test enrichment
        enriched_data = self.enricher.enrich_organization(org_dict)
        
        # Verify API call
        self.mock_github_api.get_organization.assert_called_once_with('test-org')
        
        # Verify enriched data
        self.assertEqual(enriched_data['id'], 1)
        self.assertEqual(enriched_data['login'], 'test-org')
        self.assertEqual(enriched_data['name'], 'Test Organization')
        self.assertEqual(enriched_data['description'], 'A test organization')
        self.assertEqual(enriched_data['blog'], 'https://test-org.com')
        self.assertEqual(enriched_data['location'], 'Test City')
        self.assertEqual(enriched_data['email'], 'test@test-org.com')
        self.assertEqual(enriched_data['twitter_username'], 'testorg')
        self.assertEqual(enriched_data['public_repos'], 100)
        self.assertEqual(enriched_data['followers'], 50)
        self.assertEqual(enriched_data['following'], 10)
        self.assertEqual(enriched_data['created_at'], '2020-01-01T00:00:00Z')
        self.assertEqual(enriched_data['updated_at'], '2020-01-02T00:00:00Z')
        
    def test_enrich_organization_api_error(self):
        """Test organization enrichment with API error."""
        # Test data
        org_dict = {
            'id': 1,
            'login': 'test-org'
        }
        
        # Mock API error
        self.mock_github_api.get_organization.side_effect = Exception("API Error")
        
        # Test enrichment
        enriched_data = self.enricher.enrich_organization(org_dict)
        
        # Verify API call
        self.mock_github_api.get_organization.assert_called_once_with('test-org')
        
        # Verify that original data is returned on error
        self.assertEqual(enriched_data, org_dict)
        
    def test_enrich_organization_cache(self):
        """Test organization enrichment with caching."""
        # Test data
        org_dict = {
            'id': 1,
            'login': 'test-org'
        }
        
        api_response = {
            'name': 'Test Organization',
            'description': 'A test organization'
        }
        
        # Mock API response for first call
        self.mock_github_api.get_organization.return_value = api_response
        
        # First call should use API
        enriched_data_1 = self.enricher.enrich_organization(org_dict)
        self.mock_github_api.get_organization.assert_called_once_with('test-org')
        
        # Reset mock
        self.mock_github_api.get_organization.reset_mock()
        
        # Second call should use cache
        enriched_data_2 = self.enricher.enrich_organization(org_dict)
        self.mock_github_api.get_organization.assert_not_called()
        
        # Both calls should return same data
        self.assertEqual(enriched_data_1, enriched_data_2)
        
    def tearDown(self):
        """Clean up test environment."""
        # Clean up cache directory
        cache_dir = Path("test_cache")
        if cache_dir.exists():
            for file in cache_dir.glob("*"):
                file.unlink()
            cache_dir.rmdir()
            
if __name__ == '__main__':
    unittest.main()
