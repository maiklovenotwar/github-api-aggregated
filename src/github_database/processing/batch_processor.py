"""Batch processing functionality for efficient database operations."""

import logging
import threading
import queue
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Type, Any, Tuple
from collections import defaultdict
import gc

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.exc import IntegrityError
from sqlalchemy.dialects.postgresql import insert

from ..config import ETLConfig
from ..database.database import Base, Repository, User, Event, Commit, PullRequest, Issue
from ..mapping.repository_mapper import RepositoryMapper
from ..enrichment.data_enricher import DataEnricher

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class BatchMetrics:
    """Metrics for batch processing performance monitoring."""
    start_time: float
    events_processed: int = 0
    api_calls: int = 0
    db_operations: int = 0
    errors: int = 0
    memory_usage: float = 0.0
    batch_size: int = 0
    processing_time: float = 0.0

    def calculate_throughput(self) -> float:
        """Calculate events processed per second."""
        if self.processing_time > 0:
            return self.events_processed / self.processing_time
        return 0.0

class BatchProcessor:
    """Process GitHub events in efficient batches."""

    # Mapping of event types to their respective entity classes
    EVENT_ENTITY_MAP = {
        'PushEvent': Commit,
        'PullRequestEvent': PullRequest,
        'IssuesEvent': Issue,
        'ForkEvent': Repository,
        'WatchEvent': Repository,
        'StarEvent': Repository
    }

    def __init__(self, config: ETLConfig, batch_size: int = 1000):
        """Initialize batch processor with configuration."""
        self.config = config
        self.batch_size = batch_size
        self.metrics = BatchMetrics(start_time=time.time())
        
        # Initialize thread-local storage
        self.thread_local = threading.local()
        
        # Create thread pool based on CPU cores
        self.max_workers = min(32, (os.cpu_count() or 1) * 2)
        self.executor = ThreadPoolExecutor(max_workers=self.max_workers)
        
        # Create event queues for different types
        self.event_queues = defaultdict(lambda: queue.Queue(maxsize=batch_size * 2))
        
        # Initialize database engine with optimized settings
        self.engine = create_engine(
            config.database.url,
            pool_size=self.max_workers,
            max_overflow=self.max_workers * 2,
            pool_pre_ping=True,
            pool_recycle=3600
        )
        self.Session = sessionmaker(bind=self.engine)
        
        # Create indexes for commonly queried fields
        self._create_indexes()

    def _create_indexes(self):
        """Create database indexes for performance optimization."""
        with self.engine.connect() as conn:
            # Create indexes if they don't exist
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_repository_full_name ON repository (full_name)",
                "CREATE INDEX IF NOT EXISTS idx_event_type ON event (type)",
                "CREATE INDEX IF NOT EXISTS idx_event_created_at ON event (created_at)",
                "CREATE INDEX IF NOT EXISTS idx_commit_sha ON commit (sha)",
                "CREATE INDEX IF NOT EXISTS idx_pull_request_number ON pull_request (number)",
                "CREATE INDEX IF NOT EXISTS idx_issue_number ON issue (number)"
            ]
            for idx in indexes:
                try:
                    conn.execute(text(idx))
                except Exception as e:
                    logger.warning(f"Failed to create index: {e}")
            conn.commit()

    def get_session(self) -> Session:
        """Get thread-local session."""
        if not hasattr(self.thread_local, "session"):
            self.thread_local.session = self.Session()
        return self.thread_local.session

    def _bulk_insert(self, session: Session, objects: List[Any]) -> None:
        """Perform bulk insert operation."""
        if not objects:
            return

        try:
            # Group objects by type
            by_type = defaultdict(list)
            for obj in objects:
                by_type[type(obj)].append(obj)

            # Bulk insert each type
            for obj_type, items in by_type.items():
                session.bulk_save_objects(items)
            
            session.commit()
            self.metrics.db_operations += 1
            
        except IntegrityError as e:
            session.rollback()
            logger.error(f"Bulk insert failed: {e}")
            self.metrics.errors += 1
            # Fall back to individual inserts
            for obj in objects:
                try:
                    session.merge(obj)
                    session.commit()
                except Exception as e2:
                    session.rollback()
                    logger.error(f"Individual insert failed: {e2}")
                    self.metrics.errors += 1

    def process_event_batch(self, events: List[Dict]) -> None:
        """Process a batch of events in parallel."""
        session = self.get_session()
        mapper = RepositoryMapper(session, self.config)
        
        try:
            # Group events by type
            events_by_type = defaultdict(list)
            for event in events:
                events_by_type[event['type']].append(event)

            # Process each event type in parallel
            futures = []
            for event_type, type_events in events_by_type.items():
                future = self.executor.submit(
                    self._process_event_type_batch,
                    event_type,
                    type_events,
                    mapper
                )
                futures.append(future)

            # Wait for all processing to complete
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Batch processing error: {e}")
                    self.metrics.errors += 1

            # Update metrics
            self.metrics.events_processed += len(events)
            self.metrics.batch_size = len(events)
            self.metrics.processing_time = time.time() - self.metrics.start_time
            
            # Trigger garbage collection after large batches
            if len(events) >= self.batch_size:
                gc.collect()

        finally:
            session.close()

    def _process_event_type_batch(self, event_type: str, events: List[Dict], mapper: RepositoryMapper) -> None:
        """Process a batch of events of the same type."""
        session = self.get_session()
        
        try:
            # Map events to database objects
            mapped_objects = []
            for event in events:
                try:
                    # Use appropriate mapping method based on event type
                    mapping_method = getattr(mapper, f"map_{event_type.lower()}")
                    result = mapping_method(event)
                    
                    if isinstance(result, tuple):
                        mapped_objects.extend(obj for obj in result if obj is not None)
                    else:
                        mapped_objects.append(result)
                        
                except Exception as e:
                    logger.error(f"Error mapping event {event.get('id')}: {e}")
                    self.metrics.errors += 1

            # Perform bulk insert
            if mapped_objects:
                self._bulk_insert(session, mapped_objects)

        finally:
            session.close()

    def add_event(self, event: Dict) -> None:
        """Add an event to the appropriate queue for batch processing."""
        event_type = event.get('type')
        if event_type:
            try:
                self.event_queues[event_type].put(event)
                
                # Process batch if queue is full
                if self.event_queues[event_type].qsize() >= self.batch_size:
                    self._process_queue(event_type)
            except queue.Full:
                logger.warning(f"Queue full for event type {event_type}")
                self._process_queue(event_type)

    def _process_queue(self, event_type: str) -> None:
        """Process all events in a queue."""
        events = []
        queue = self.event_queues[event_type]
        
        while not queue.empty() and len(events) < self.batch_size:
            try:
                events.append(queue.get_nowait())
            except queue.Empty:
                break

        if events:
            self.process_event_batch(events)

    def flush_queues(self) -> None:
        """Process all remaining events in queues."""
        for event_type in self.event_queues:
            self._process_queue(event_type)

    def get_metrics(self) -> Dict[str, Any]:
        """Get current processing metrics."""
        return {
            "events_processed": self.metrics.events_processed,
            "api_calls": self.metrics.api_calls,
            "db_operations": self.metrics.db_operations,
            "errors": self.metrics.errors,
            "throughput": self.metrics.calculate_throughput(),
            "batch_size": self.metrics.batch_size,
            "processing_time": self.metrics.processing_time
        }

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit with cleanup."""
        self.flush_queues()
        self.executor.shutdown(wait=True)
        for session in getattr(self.thread_local, "session", []):
            session.close()
