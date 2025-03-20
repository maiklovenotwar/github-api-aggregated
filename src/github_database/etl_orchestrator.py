"""ETL orchestration for GitHub data processing."""

import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Generator, Dict, Any
import json
import gc

from .config import ETLConfig
from .database.database import create_tables
from .github_archive.github_archive import GithubArchive
from .processing.batch_processor import BatchProcessor
from .control_database.validate_data import DataValidator

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ETLOrchestrator:
    """Orchestrate the ETL process for GitHub data."""

    def __init__(self, config: ETLConfig):
        """Initialize orchestrator with configuration."""
        self.config = config
        self.archive = GithubArchive()
        self.validator = DataValidator()
        
        # Create database tables
        create_tables(config.database.url)
        
        # Initialize batch processor with optimal batch size
        self.batch_processor = BatchProcessor(
            config,
            batch_size=self.calculate_optimal_batch_size()
        )

    def calculate_optimal_batch_size(self) -> int:
        """Calculate optimal batch size based on system resources."""
        import psutil
        
        # Get system memory info
        memory = psutil.virtual_memory()
        
        # Base batch size on available memory (aim to use max 25% of available memory)
        # Assuming average event size of 1KB
        available_memory = memory.available * 0.25  # 25% of available memory
        optimal_size = int(available_memory / 1024)  # Convert to KB
        
        # Clamp between reasonable limits
        return max(100, min(optimal_size, 5000))

    def process_archive(self, start_date: datetime, end_date: datetime) -> None:
        """Process GitHub Archive data for the given date range."""
        try:
            current_date = start_date
            while current_date <= end_date:
                logger.info(f"Processing data for {current_date}")
                
                # Stream events for the current date
                for event_batch in self._stream_events(current_date):
                    # Validate events before processing
                    valid_events = self.validator.validate_events(event_batch)
                    
                    # Process valid events in batch
                    for event in valid_events:
                        self.batch_processor.add_event(event)
                    
                    # Log progress
                    self._log_progress()
                
                # Ensure all events are processed
                self.batch_processor.flush_queues()
                
                # Move to next date
                current_date += timedelta(days=1)
                
                # Force garbage collection between dates
                gc.collect()
                
        except Exception as e:
            logger.error(f"Error in ETL process: {e}")
            raise
        finally:
            # Log final metrics
            self._log_final_metrics()

    def _stream_events(self, date: datetime) -> Generator[Dict[str, Any], None, None]:
        """Stream events from archive in memory-efficient chunks."""
        for archive_url in self.archive.get_archive_urls(date):
            try:
                # Download and decompress archive
                with self.archive.download_archive(archive_url) as archive_file:
                    batch = []
                    
                    # Process archive line by line
                    for line in archive_file:
                        try:
                            event = json.loads(line)
                            batch.append(event)
                            
                            # Yield batch when it reaches sufficient size
                            if len(batch) >= 100:  # Small batch for streaming
                                yield batch
                                batch = []
                                
                        except json.JSONDecodeError as e:
                            logger.warning(f"Invalid JSON in archive: {e}")
                            continue
                    
                    # Yield remaining events
                    if batch:
                        yield batch
                        
            except Exception as e:
                logger.error(f"Error processing archive {archive_url}: {e}")
                continue

    def _log_progress(self) -> None:
        """Log processing progress and metrics."""
        metrics = self.batch_processor.get_metrics()
        logger.info(
            f"Progress: {metrics['events_processed']} events processed, "
            f"Throughput: {metrics['throughput']:.2f} events/sec, "
            f"Errors: {metrics['errors']}"
        )

    def _log_final_metrics(self) -> None:
        """Log final processing metrics."""
        metrics = self.batch_processor.get_metrics()
        logger.info("Final processing metrics:")
        logger.info(f"Total events processed: {metrics['events_processed']}")
        logger.info(f"Average throughput: {metrics['throughput']:.2f} events/sec")
        logger.info(f"Total database operations: {metrics['db_operations']}")
        logger.info(f"Total API calls: {metrics['api_calls']}")
        logger.info(f"Total errors: {metrics['errors']}")
        logger.info(f"Total processing time: {metrics['processing_time']:.2f} seconds")
