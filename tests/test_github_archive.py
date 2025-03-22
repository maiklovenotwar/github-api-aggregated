"""Tests for GitHub Archive processing functionality."""

import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime
from pathlib import Path
import tempfile
import gzip
import json
import responses
from src.github_database.github_archive.github_archive import GitHubArchiveProcessor

class TestGitHubArchiveProcessor(unittest.TestCase):
    """Test cases for GitHubArchiveProcessor class."""
    
    def setUp(self):
        """Set up test environment."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.processor = GitHubArchiveProcessor(cache_dir=self.temp_dir)
        self.test_date = datetime(2025, 3, 20)
        
    def tearDown(self):
        """Clean up test environment."""
        for file in self.temp_dir.glob("*"):
            file.unlink()
        self.temp_dir.rmdir()
        
    def test_get_archive_urls(self):
        """Test URL generation for archives."""
        urls = self.processor.get_archive_urls(self.test_date)
        self.assertEqual(len(urls), 24)  # One URL per hour
        
        # Check URL format
        expected_base = "https://data.gharchive.org/2025-03-20"
        for hour, url in enumerate(urls):
            self.assertEqual(url, f"{expected_base}-{hour}.json.gz")
            
    @responses.activate
    def test_stream_events(self):
        """Test event streaming from archive."""
        # Create test data
        test_events = [
            {"type": "PushEvent", "id": "1"},
            {"type": "PullRequestEvent", "id": "2"}
        ]
        
        # Prepare mock response
        hour = 0
        url = f"https://data.gharchive.org/2025-03-20-{hour}.json.gz"
        
        # Create gzipped content
        content = "\n".join(json.dumps(event) for event in test_events)
        gzipped_content = gzip.compress(content.encode())
        
        responses.add(
            responses.GET,
            url,
            body=gzipped_content,
            status=200,
            content_type="application/gzip"
        )
        
        # Test streaming
        events = list(self.processor.stream_events(self.test_date))
        self.assertEqual(len(events), 2)
        self.assertEqual(events[0]["type"], "PushEvent")
        self.assertEqual(events[1]["type"], "PullRequestEvent")
        
    def test_cache_functionality(self):
        """Test caching of downloaded files."""
        # Create test file in cache
        test_file = self.temp_dir / "2025-03-20-0.json.gz"
        test_events = [
            {"type": "PushEvent", "id": "1"},
            {"type": "PullRequestEvent", "id": "2"}
        ]
        
        content = "\n".join(json.dumps(event) for event in test_events)
        with gzip.open(test_file, "wt") as f:
            f.write(content)
            
        # Stream events (should use cache)
        events = list(self.processor._process_cached_file(test_file))
        self.assertEqual(len(events), 2)
        self.assertEqual(events[0]["type"], "PushEvent")
        
    def test_clean_cache(self):
        """Test cache cleaning functionality."""
        # Create old and new test files
        old_file = self.temp_dir / "old.json.gz"
        new_file = self.temp_dir / "new.json.gz"
        
        old_file.touch()
        new_file.touch()
        
        # Set old file's mtime to 10 days ago
        old_timestamp = (datetime.now().timestamp() - (10 * 24 * 60 * 60))
        old_file.stat().st_mtime = old_timestamp
        
        # Clean cache with 7 days max age
        self.processor.clean_cache(max_age_days=7)
        
        # Check results
        self.assertFalse(old_file.exists())
        self.assertTrue(new_file.exists())
        
    def test_get_event_counts(self):
        """Test event counting functionality."""
        test_events = [
            {"type": "PushEvent", "id": "1"},
            {"type": "PushEvent", "id": "2"},
            {"type": "PullRequestEvent", "id": "3"}
        ]
        
        # Mock stream_events to return test data
        with patch.object(self.processor, 'stream_events', return_value=test_events):
            counts = self.processor.get_event_counts(self.test_date)
            
            self.assertEqual(counts["PushEvent"], 2)
            self.assertEqual(counts["PullRequestEvent"], 1)
            
    def test_error_handling(self):
        """Test error handling in event processing."""
        # Create corrupted gzip file
        test_file = self.temp_dir / "corrupted.json.gz"
        with open(test_file, "wb") as f:
            f.write(b"corrupted data")
            
        # Test processing of corrupted file
        events = list(self.processor._process_cached_file(test_file))
        self.assertEqual(len(events), 0)  # Should handle error and return empty list

if __name__ == '__main__':
    unittest.main()
