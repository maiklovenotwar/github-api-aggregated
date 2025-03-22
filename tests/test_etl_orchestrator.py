"""Tests for ETL orchestration functionality."""

import unittest
from unittest.mock import patch, MagicMock, call
from datetime import datetime
import tempfile
from pathlib import Path

from src.github_database.etl_orchestrator import ETLOrchestrator
from src.github_database.config import ETLConfig

class TestETLOrchestrator(unittest.TestCase):
    """Test cases for ETLOrchestrator class."""
    
    def setUp(self):
        """Set up test environment."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.config = ETLConfig(
            cache_dir=str(self.temp_dir),
            batch_size=100,
            github_token="test_token",
            database_url="sqlite:///:memory:"
        )
        self.orchestrator = ETLOrchestrator(self.config)
        
    def tearDown(self):
        """Clean up test environment."""
        for file in self.temp_dir.glob("*"):
            file.unlink()
        self.temp_dir.rmdir()
        
    def test_initialization(self):
        """Test proper initialization of components."""
        self.assertIsNotNone(self.orchestrator.archive_processor)
        self.assertIsNotNone(self.orchestrator.batch_processor)
        self.assertIsNotNone(self.orchestrator.data_validator)
        self.assertIsNotNone(self.orchestrator.data_enricher)
        
    @patch('src.github_database.etl_orchestrator.GitHubArchiveProcessor')
    @patch('src.github_database.etl_orchestrator.BatchProcessor')
    @patch('src.github_database.etl_orchestrator.DataValidator')
    @patch('src.github_database.etl_orchestrator.DataEnricher')
    def test_process_single_date(self, mock_enricher, mock_validator, mock_processor, mock_archive):
        """Test processing of a single date."""
        # Setup mock data
        test_date = datetime(2025, 3, 20)
        test_events = [{"id": "1"}, {"id": "2"}]
        
        # Configure mocks
        mock_archive.return_value.stream_events.return_value = test_events
        mock_validator.return_value.validate_events.return_value = test_events
        mock_enricher.return_value.batch_enrich_events.return_value = test_events
        
        # Process date
        self.orchestrator.process_single_date(test_date)
        
        # Verify calls
        mock_validator.return_value.validate_events.assert_called_once()
        mock_enricher.return_value.batch_enrich_events.assert_called_once()
        mock_processor.return_value.process_event_batch.assert_called_once()
        
    def test_process_date_range(self):
        """Test processing of date range."""
        start_date = datetime(2025, 3, 20)
        end_date = datetime(2025, 3, 21)
        
        with patch.object(self.orchestrator, 'process_single_date') as mock_process:
            self.orchestrator.process_date_range(start_date, end_date)
            
            # Should be called twice (one for each day)
            self.assertEqual(mock_process.call_count, 2)
            mock_process.assert_has_calls([
                call(datetime(2025, 3, 20)),
                call(datetime(2025, 3, 21))
            ])
            
    def test_get_processing_metrics(self):
        """Test metrics collection."""
        # Mock batch processor metrics
        mock_metrics = {
            "events_processed": 100,
            "api_calls": 50,
            "db_operations": 75,
            "errors": 5,
            "throughput": 10.5,
            "memory_usage_mb": 256,
            "processing_time_seconds": 9.5
        }
        
        with patch.object(self.orchestrator.batch_processor, 'get_metrics', 
                         return_value=MagicMock(**mock_metrics)):
            metrics = self.orchestrator.get_processing_metrics()
            
            self.assertEqual(metrics["events_processed"], 100)
            self.assertEqual(metrics["api_calls"], 50)
            self.assertEqual(metrics["db_operations"], 75)
            self.assertEqual(metrics["errors"], 5)
            self.assertEqual(metrics["throughput"], 10.5)
            self.assertEqual(metrics["memory_usage_mb"], 256)
            self.assertEqual(metrics["processing_time_seconds"], 9.5)
            
    def test_error_handling(self):
        """Test error handling during processing."""
        test_date = datetime(2025, 3, 20)
        
        # Mock stream_event_batches to raise an exception
        with patch.object(self.orchestrator, '_stream_event_batches', 
                         side_effect=Exception("Test error")):
            # Should not raise exception
            self.orchestrator.process_single_date(test_date)
            
    def test_cleanup(self):
        """Test cleanup functionality."""
        with patch.object(self.orchestrator.archive_processor, 'clean_cache') as mock_clean:
            self.orchestrator.cleanup()
            mock_clean.assert_called_once()
            
    def test_stream_event_batches(self):
        """Test event batch streaming."""
        test_date = datetime(2025, 3, 20)
        test_events = [{"id": str(i)} for i in range(150)]  # 150 events
        
        # Mock stream_events to return test data
        with patch.object(self.orchestrator.archive_processor, 'stream_events',
                         return_value=test_events):
            # Get batches (batch_size=100 from config)
            batches = list(self.orchestrator._stream_event_batches(test_date))
            
            # Should have 2 batches (100 and 50 events)
            self.assertEqual(len(batches), 2)
            self.assertEqual(len(batches[0]), 100)
            self.assertEqual(len(batches[1]), 50)

if __name__ == '__main__':
    unittest.main()
