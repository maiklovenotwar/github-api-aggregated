"""Tests for data enrichment functionality."""

import unittest
import tempfile
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

from src.github_database.enrichment.data_enricher import (
    DataEnricher,
    Cache,
    CacheConfig
)
from src.github_database.config import ETLConfig
from src.github_database.api.github_api import GitHubAPI

class TestCache(unittest.TestCase):
    """Test cases for multi-level cache."""
    
    def setUp(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.config = CacheConfig(
            memory_cache_size=2,  # Small size for testing eviction
            disk_cache_size_bytes=1024,
            cache_ttl=3600,
            batch_size=10
        )
        self.cache = Cache(self.config, Path(self.temp_dir))
        
    def test_cache_set_get(self):
        """Test basic cache set and get operations."""
        # Set value in cache
        self.cache.set('test_key', {'data': 'test_value'})
        
        # Get value from cache
        value = self.cache.get('test_key')
        self.assertEqual(value['data'], 'test_value')
        
    def test_memory_cache_eviction(self):
        """Test memory cache size limit and eviction."""
        # Add items to fill cache
        self.cache.set('key1', 'value1')
        self.cache.set('key2', 'value2')
        
        # Access key1 to increase its access count
        self.cache.get('key1')
        
        # Add another item to trigger eviction
        self.cache.set('key3', 'value3')
        
        # key2 should be evicted (least accessed)
        self.assertIsNone(self.cache.get('key2'))
        self.assertIsNotNone(self.cache.get('key1'))
        self.assertIsNotNone(self.cache.get('key3'))
        
    def test_cache_invalidation(self):
        """Test cache invalidation."""
        self.cache.set('test_key', 'test_value')
        self.cache.invalidate('test_key')
        
        self.assertIsNone(self.cache.get('test_key'))
        
    def test_cache_clear(self):
        """Test clearing all cache levels."""
        self.cache.set('key1', 'value1')
        self.cache.set('key2', 'value2')
        
        self.cache.clear()
        
        self.assertIsNone(self.cache.get('key1'))
        self.assertIsNone(self.cache.get('key2'))

class TestDataEnricher(unittest.TestCase):
    """Test cases for DataEnricher."""
    
    def setUp(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.config = Mock(spec=ETLConfig)
        self.config.api = Mock(
            token='test_token',
            rate_limit_delay=0.1,
            parallel_requests=2
        )
        self.enricher = DataEnricher(self.config, Path(self.temp_dir))
        
    def test_enrich_repository(self):
        """Test repository enrichment."""
        repo_dict = {
            'id': 1,
            'name': 'owner/repo'
        }
        
        # Mock API response
        api_response = {
            'description': 'Test repo',
            'language': 'Python',
            'stargazers_count': 100,
            'forks_count': 50,
            'created_at': '2024-01-01T00:00:00Z',
            'updated_at': '2024-01-01T00:00:00Z',
            'topics': ['test']
        }
        
        with patch.object(GitHubAPI, 'get_repository', return_value=api_response):
            enriched = self.enricher.enrich_repository(repo_dict)
            
            self.assertEqual(enriched['description'], 'Test repo')
            self.assertEqual(enriched['language'], 'Python')
            self.assertEqual(enriched['stars'], 100)
            self.assertEqual(enriched['forks'], 50)
            
    def test_enrich_user(self):
        """Test user enrichment."""
        user_dict = {
            'id': 1,
            'login': 'testuser'
        }
        
        # Mock API response
        api_response = {
            'name': 'Test User',
            'email': 'test@example.com',
            'company': 'Test Corp',
            'location': 'Test City',
            'created_at': '2024-01-01T00:00:00Z',
            'updated_at': '2024-01-01T00:00:00Z'
        }
        
        with patch.object(GitHubAPI, 'get_user', return_value=api_response):
            enriched = self.enricher.enrich_user(user_dict)
            
            self.assertEqual(enriched['name'], 'Test User')
            self.assertEqual(enriched['email'], 'test@example.com')
            self.assertEqual(enriched['company'], 'Test Corp')
            
    def test_enrich_commit(self):
        """Test commit enrichment."""
        commit_dict = {
            'sha': 'abc123',
            'message': 'Test commit'
        }
        
        # Mock API response
        api_response = {
            'stats': {'additions': 10, 'deletions': 5},
            'files': [{'filename': 'test.py'}],
            'author': {'name': 'Test Author'},
            'committer': {'name': 'Test Committer'}
        }
        
        with patch.object(GitHubAPI, 'get_commit', return_value=api_response):
            enriched = self.enricher.enrich_commit(commit_dict, 'owner/repo')
            
            self.assertEqual(enriched['stats']['additions'], 10)
            self.assertEqual(enriched['stats']['deletions'], 5)
            self.assertEqual(len(enriched['files']), 1)
            
    def test_enrich_pull_request(self):
        """Test pull request enrichment."""
        pr_dict = {
            'number': 1,
            'title': 'Test PR'
        }
        
        # Mock API response
        api_response = {
            'merged_by': {'login': 'merger'},
            'review_comments': 5,
            'commits': 3,
            'additions': 100,
            'deletions': 50,
            'changed_files': 3
        }
        
        with patch.object(GitHubAPI, 'get_pull_request', return_value=api_response):
            enriched = self.enricher.enrich_pull_request(pr_dict, 'owner/repo')
            
            self.assertEqual(enriched['review_comments'], 5)
            self.assertEqual(enriched['commits'], 3)
            self.assertEqual(enriched['additions'], 100)
            
    def test_enrich_issue(self):
        """Test issue enrichment."""
        issue_dict = {
            'number': 1,
            'title': 'Test Issue'
        }
        
        # Mock API response
        api_response = {
            'labels': [{'name': 'bug'}],
            'assignees': [{'login': 'assignee'}],
            'comments': 10,
            'closed_by': {'login': 'closer'}
        }
        
        with patch.object(GitHubAPI, 'get_issue', return_value=api_response):
            enriched = self.enricher.enrich_issue(issue_dict, 'owner/repo')
            
            self.assertEqual(len(enriched['labels']), 1)
            self.assertEqual(len(enriched['assignees']), 1)
            self.assertEqual(enriched['comments'], 10)
            
    def test_batch_enrich_events(self):
        """Test batch enrichment of events."""
        events = [
            {
                'id': '1',
                'type': 'PushEvent',
                'actor': {'id': 1, 'login': 'user1'},
                'repo': {'id': 1, 'name': 'owner/repo1'},
                'payload': {
                    'commits': [
                        {'sha': 'abc123', 'message': 'Test commit'}
                    ]
                }
            },
            {
                'id': '2',
                'type': 'PullRequestEvent',
                'actor': {'id': 2, 'login': 'user2'},
                'repo': {'id': 2, 'name': 'owner/repo2'},
                'payload': {
                    'pull_request': {
                        'number': 1,
                        'title': 'Test PR'
                    }
                }
            }
        ]
        
        # Mock API responses
        mock_responses = {
            'repo': {'description': 'Test repo'},
            'user': {'name': 'Test User'},
            'commit': {'stats': {'additions': 10}},
            'pull_request': {'review_comments': 5}
        }
        
        with patch.object(GitHubAPI, 'get_repository', return_value=mock_responses['repo']), \
             patch.object(GitHubAPI, 'get_user', return_value=mock_responses['user']), \
             patch.object(GitHubAPI, 'get_commit', return_value=mock_responses['commit']), \
             patch.object(GitHubAPI, 'get_pull_request', return_value=mock_responses['pull_request']):
            
            enriched = self.enricher.batch_enrich_events(events)
            
            self.assertEqual(len(enriched), 2)
            self.assertEqual(enriched[0]['type'], 'PushEvent')
            self.assertEqual(enriched[1]['type'], 'PullRequestEvent')
            
    def test_rate_limiting(self):
        """Test API rate limiting."""
        start_time = datetime.now()
        
        # Make multiple API calls
        for _ in range(3):
            self.enricher.enrich_repository({'id': 1, 'name': 'owner/repo'})
            
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        # Should take at least 0.2 seconds due to rate limiting
        self.assertGreaterEqual(duration, 0.2)
        
if __name__ == '__main__':
    unittest.main()
