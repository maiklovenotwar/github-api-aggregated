"""Tests for GitHub Archive processing functionality."""

import unittest
from datetime import datetime, timedelta
from pathlib import Path
import tempfile
import shutil
from unittest.mock import Mock, patch

from src.github_database.github_archive.github_archive import (
    GitHubArchiveProcessor,
    QualityFilters,
    RepositoryMetrics
)

class TestGitHubArchiveProcessor(unittest.TestCase):
    """Test cases for GitHubArchiveProcessor class."""
    
    def setUp(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.mock_api = Mock()
        self.mock_db = Mock()
        
        # Create processor with temporary cache directory
        self.processor = GitHubArchiveProcessor(self.mock_api, self.mock_db)
        self.processor.CACHE_DIR = Path(self.temp_dir)
        
    def tearDown(self):
        """Clean up test environment."""
        shutil.rmtree(self.temp_dir)
        
    @patch('requests.get')
    def test_download_daily_archive(self, mock_get):
        """Test downloading daily archive files."""
        # Mock successful download
        mock_get.return_value.status_code = 200
        mock_get.return_value.iter_content.return_value = [b'test data']
        
        date = datetime(2024, 1, 1)
        files = self.processor.download_daily_archive(date)
        
        self.assertEqual(len(files), 24)  # 24 hourly files
        self.assertTrue(all(f.exists() for f in files))
        
    def test_filter_events_by_repo_quality(self):
        """Test filtering events based on repository quality."""
        # Sample test data
        events = [
            {'repo': {'id': 1}, 'type': 'PushEvent'},
            {'repo': {'id': 2}, 'type': 'PullRequestEvent'},
        ]
        
        # Mock repository metrics
        self.processor.fetch_repo_metrics = Mock(return_value={
            1: RepositoryMetrics(
                id=1,
                stars=100,
                forks=20,
                commits_last_year=150,
                language='Python',
                last_updated=datetime.now()
            ),
            2: RepositoryMetrics(
                id=2,
                stars=10,  # Below threshold
                forks=5,   # Below threshold
                commits_last_year=50,  # Below threshold
                language='JavaScript',
                last_updated=datetime.now()
            )
        })
        
        filters = QualityFilters(
            min_stars=50,
            min_forks=10,
            min_commits_last_year=100
        )
        
        filtered_events = self.processor.filter_events_by_repo_quality(events, filters)
        
        self.assertEqual(len(filtered_events), 1)
        self.assertEqual(filtered_events[0]['repo']['id'], 1)
        
    def test_innovation_classification(self):
        """Test classification of innovation-relevant events."""
        # Test new project creation
        create_event = {
            'type': 'CreateEvent',
            'payload': {'ref_type': 'repository'}
        }
        self.assertEqual(
            self.processor._classify_innovation_event(create_event),
            'new_project'
        )
        
        # Test feature innovation in PR
        pr_event = {
            'type': 'PullRequestEvent',
            'payload': {
                'pull_request': {
                    'title': 'Implement new AI feature',
                    'body': 'This PR introduces a new machine learning component'
                }
            }
        }
        self.assertEqual(
            self.processor._classify_innovation_event(pr_event),
            'feature_innovation'
        )
        
        # Test major release
        release_event = {
            'type': 'ReleaseEvent',
            'payload': {
                'release': {'tag_name': 'v2.0.0'}
            }
        }
        self.assertEqual(
            self.processor._classify_innovation_event(release_event),
            'major_release'
        )
        
if __name__ == '__main__':
    unittest.main()
