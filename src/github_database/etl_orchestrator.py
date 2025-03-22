"""ETL orchestration for GitHub data processing."""

import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Generator, Dict, Any, List
import json
import gc

from .config import ETLConfig
from .database.database import create_tables
from .github_archive.github_archive import GitHubArchiveProcessor
from .processing.batch_processor import BatchProcessor
from .control_database.validate_data import DataValidator
from .enrichment.data_enricher import DataEnricher

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ETLOrchestrator:
    """Orchestrate the ETL process for GitHub data."""
    
    def __init__(self, config: ETLConfig):
        """
        Initialize ETL orchestrator.
        
        Args:
            config: ETL configuration
        """
        self.config = config
        
        # Initialize components
        self.archive_processor = GitHubArchiveProcessor(
            cache_dir=Path(config.cache_dir) / "github_archive"
        )
        
        self.batch_processor = BatchProcessor(
            config=config,
            batch_size=config.batch_size
        )
        
        self.data_validator = DataValidator()
        self.data_enricher = DataEnricher(config)
        
        # Ensure database tables exist
        create_tables()
        
    def process_date_range(self, start_date: datetime, end_date: datetime):
        """
        Process GitHub events for a date range.
        
        Args:
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
        """
        current_date = start_date
        while current_date <= end_date:
            try:
                self.process_single_date(current_date)
            except Exception as e:
                logger.error(f"Failed to process date {current_date}: {e}")
            current_date += timedelta(days=1)
            
    def process_single_date(self, date: datetime):
        """
        Process GitHub events for a single date.
        
        Args:
            date: Date to process
        """
        logger.info(f"Processing events for {date.strftime('%Y-%m-%d')}")
        
        try:
            # Stream and process events
            for event_batch in self._stream_event_batches(date):
                try:
                    # Validate events
                    valid_events = self.data_validator.validate_events(event_batch)
                    
                    # Enrich valid events
                    enriched_events = self.data_enricher.batch_enrich_events(valid_events)
                    
                    # Process enriched events
                    self.batch_processor.process_event_batch(enriched_events)
                    
                except Exception as e:
                    logger.error(f"Failed to process batch: {e}")
                    continue
                    
                # Perform garbage collection after each batch
                gc.collect()
                
        finally:
            # Ensure all remaining events are processed
            self.batch_processor.flush_queues()
            
    def _stream_event_batches(self, date: datetime) -> Generator[List[Dict], None, None]:
        """
        Stream events in batches.
        
        Args:
            date: Date to stream events for
            
        Yields:
            List[Dict]: Batch of events
        """
        batch = []
        for event in self.archive_processor.stream_events(date):
            batch.append(event)
            
            if len(batch) >= self.config.batch_size:
                yield batch
                batch = []
                
        # Yield remaining events
        if batch:
            yield batch
            
    def get_processing_metrics(self) -> Dict[str, Any]:
        """
        Get processing metrics.
        
        Returns:
            Dict with processing metrics
        """
        metrics = self.batch_processor.get_metrics()
        return {
            "events_processed": metrics.events_processed,
            "api_calls": metrics.api_calls,
            "db_operations": metrics.db_operations,
            "errors": metrics.errors,
            "throughput": metrics.calculate_throughput(),
            "memory_usage_mb": metrics.memory_usage,
            "processing_time_seconds": metrics.processing_time
        }
        
    def cleanup(self):
        """Cleanup resources and temporary files."""
        # Clean old cache files
        self.archive_processor.clean_cache()
        
        # Perform garbage collection
        gc.collect()
