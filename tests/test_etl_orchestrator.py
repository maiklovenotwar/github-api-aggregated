"""Tests for ETL Orchestrator."""

import unittest
from datetime import datetime, timedelta
from pathlib import Path
import tempfile
import shutil
from unittest.mock import Mock, patch, MagicMock

from src.github_database.config import (
    ETLConfig,
    APIConfig,
    ArchiveConfig,
    DatabaseConfig,
    QualityThresholds,
    ProcessingState
)
from src.github_database.etl_orchestrator import ETLOrchestrator

class TestETLOrchestrator(unittest.TestCase):
    """Test cases for ETLOrchestrator."""
    
    def setUp(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.config = ETLConfig(
            quality=QualityThresholds(
                min_stars=50,
                min_forks=10,
                min_commits_last_year=100
            ),
            api=APIConfig(token="test_token"),
            archive=ArchiveConfig(cache_dir=Path(self.temp_dir)),
            database=DatabaseConfig(url="sqlite:///:memory:"),
            start_date=datetime.now() - timedelta(days=1),
            end_date=datetime.now(),
            state_file=Path(self.temp_dir) / "state.json"
        )
        
        # Mock components
        self.mock_github_api = Mock()
        self.mock_database = Mock()
        self.mock_archive_processor = Mock()
        
        # Create orchestrator
        self.orchestrator = ETLOrchestrator(self.config)
        self.orchestrator.github_api = self.mock_github_api
        self.orchestrator.database = self.mock_database
        self.orchestrator.archive_processor = self.mock_archive_processor
        
    def tearDown(self):
        """Clean up test environment."""
        shutil.rmtree(self.temp_dir)
        
    def test_process_archives(self):
        """Test archive processing workflow."""
        # Mock archive data
        mock_events = [
            {
                'id': '1',
                'type': 'PushEvent',
                'actor': {'id': 1},
                'repo': {'id': 1},
                'payload': {},
                'created_at': '2024-01-01T00:00:00Z',
                'public': True
            }
        ]
        
        self.mock_archive_processor.download_daily_archive.return_value = [
            Path("test1.json.gz")
        ]
        self.mock_archive_processor.process_archive_file.return_value = mock_events
        self.mock_archive_processor.filter_events_by_repo_quality.return_value = mock_events
        
        # Create mock session
        mock_session = MagicMock()
        self.mock_database.get_session.return_value.__enter__.return_value = mock_session
        
        # Run processing
        self.orchestrator._process_archives()
        
        # Verify calls
        self.mock_archive_processor.download_daily_archive.assert_called()
        self.mock_archive_processor.process_archive_file.assert_called()
        self.mock_archive_processor.filter_events_by_repo_quality.assert_called()
        mock_session.execute.assert_called()  # Verify bulk insert
        
    def test_enrich_with_api_data(self):
        """Test API data enrichment."""
        # Mock repository data
        mock_repo = Mock(id=1, stars=None)
        mock_session = MagicMock()
        mock_session.query().filter().all.return_value = [(1,)]
        mock_session.query().get.return_value = mock_repo
        
        self.mock_database.get_session.return_value.__enter__.return_value = mock_session
        
        # Mock API response
        self.mock_archive_processor.fetch_repo_metrics.return_value = {
            1: Mock(
                stars=100,
                forks=20,
                language='Python'
            )
        }
        
        # Run enrichment
        self.orchestrator._enrich_with_api_data()
        
        # Verify repository was updated
        self.assertEqual(mock_repo.stars, 100)
        self.assertEqual(mock_repo.forks, 20)
        self.assertEqual(mock_repo.language, 'Python')
        
    def test_state_management(self):
        """Test processing state management."""
        # Create initial state
        initial_state = ProcessingState(
            last_processed_date=datetime(2024, 1, 1),
            last_processed_hour=5,
            processed_repo_ids={1, 2},
            failed_repo_ids={3},
            event_counts={'PushEvent': 10}
        )
        
        # Save state
        self.config.save_state(initial_state)
        
        # Create new orchestrator to load state
        new_orchestrator = ETLOrchestrator(self.config)
        
        # Verify state was loaded correctly
        loaded_state = new_orchestrator.state
        self.assertEqual(loaded_state.last_processed_date, initial_state.last_processed_date)
        self.assertEqual(loaded_state.last_processed_hour, initial_state.last_processed_hour)
        self.assertEqual(loaded_state.processed_repo_ids, initial_state.processed_repo_ids)
        self.assertEqual(loaded_state.failed_repo_ids, initial_state.failed_repo_ids)
        self.assertEqual(loaded_state.event_counts, initial_state.event_counts)
        
    def test_error_handling(self):
        """Test error handling and recovery."""
        # Mock error in archive processing
        self.mock_archive_processor.download_daily_archive.side_effect = Exception("Test error")
        
        # Verify error is logged and state is saved
        with self.assertRaises(Exception):
            self.orchestrator.run()
            
        # Verify state was saved despite error
        self.assertTrue(self.config.state_file.exists())
        
if __name__ == '__main__':
    unittest.main()
