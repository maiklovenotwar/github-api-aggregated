"""Batch processing functionality for efficient database operations."""

import logging
import threading
import queue
import time
import os
import gc
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Type, Any, Tuple
from collections import defaultdict

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.exc import IntegrityError
from sqlalchemy.dialects.postgresql import insert

from ..config import ETLConfig
from ..database.database import (
    Base, Repository, User, Event, Commit, PullRequest, Issue,
    create_tables, CommitData, PushEventData, PullRequestEventData,
    IssueEventData, ForkEventData, WatchEventData
)
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
    memory_peak: float = 0.0
    batch_size: int = 0
    processing_time: float = 0.0
    repositories: int = 0
    events: int = 0

    @property
    def duration(self) -> float:
        """Calculate total duration in seconds."""
        return time.time() - self.start_time

    def calculate_throughput(self) -> float:
        """Calculate events processed per second."""
        if self.processing_time > 0:
            return self.events_processed / self.processing_time
        return 0.0


class BatchProcessor:
    """Process GitHub events in efficient batches."""

    EVENT_ENTITY_MAP = {
        'PushEvent': Event,
        'PullRequestEvent': Event,
        'IssuesEvent': Event,
        'ForkEvent': Event,
        'WatchEvent': Event
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
        
        # Create database tables and indexes
        Base.metadata.create_all(self.engine)
        self._create_indexes()
        
    def _create_indexes(self):
        """Create database indexes for performance optimization."""
        with self.get_session() as session:
            session.execute(text('CREATE INDEX IF NOT EXISTS idx_repo_id ON events (repo_id)'))
            session.execute(text('CREATE INDEX IF NOT EXISTS idx_actor_id ON events (actor_id)'))
            session.execute(text('CREATE INDEX IF NOT EXISTS idx_sha ON commits (sha)'))
            session.execute(text('CREATE INDEX IF NOT EXISTS idx_pr_number ON pull_requests (number)'))
            session.execute(text('CREATE INDEX IF NOT EXISTS idx_issue_number ON issues (number)'))
            session.commit()

    def get_session(self) -> Session:
        """Get thread-local session."""
        if not hasattr(self.thread_local, 'session'):
            self.thread_local.session = self.Session()
        return self.thread_local.session

    def _bulk_insert(self, session: Session, objects: List[Any]):
        """Perform bulk insert operation."""
        if not objects:
            return
            
        try:
            session.bulk_save_objects(objects)
            session.commit()
            self.metrics.db_operations += len(objects)
        except IntegrityError as e:
            session.rollback()
            logger.warning(f"Bulk insert failed, falling back to individual inserts: {e}")
            
            # Fall back to individual inserts
            for obj in objects:
                try:
                    session.merge(obj)
                    session.commit()
                    self.metrics.db_operations += 1
                except Exception as e:
                    session.rollback()
                    logger.error(f"Failed to insert object: {e}")
                    self.metrics.errors += 1

    def process_event_batch(self, events: List[Dict]):
        """Process a batch of events in parallel."""
        if not events:
            return
            
        # Group events by type for efficient processing
        event_groups = defaultdict(list)
        for event in events:
            event_type = event.get('type')
            if event_type in self.EVENT_ENTITY_MAP:
                event_groups[event_type].append(event)

        # Process each event type in parallel
        futures = []
        for event_type, type_events in event_groups.items():
            future = self.executor.submit(self._process_event_type_batch, event_type, type_events)
            futures.append(future)

        # Wait for all processing to complete
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error(f"Batch processing failed: {e}")
                self.metrics.errors += 1

        # Update metrics
        self.metrics.events_processed += len(events)
        self.metrics.events = self.metrics.events_processed
        self.metrics.repositories = len(set(event['repo']['id'] for event in events if 'repo' in event))
        self.metrics.processing_time = time.time() - self.metrics.start_time
        current_memory = self._get_memory_usage()
        self.metrics.memory_usage = current_memory
        self.metrics.memory_peak = max(self.metrics.memory_peak, current_memory)

        # Perform garbage collection after large batches
        if len(events) >= self.batch_size:
            gc.collect()

    def _process_event_type_batch(self, event_type: str, events: List[Dict]):
        """Process a batch of events of the same type."""
        session = self.get_session()
        try:
            # Map events to database objects
            entity_class = self.EVENT_ENTITY_MAP[event_type]
            mapper = RepositoryMapper(session, self.config, DataEnricher(self.config, self.config.cache_dir))
            objects = [mapper.map_event_to_entity(event, entity_class) for event in events]
            
            # Filter out None values
            objects = [obj for obj in objects if obj is not None]
            
            # Bulk insert objects
            self._bulk_insert(session, objects)
            
        except Exception as e:
            logger.error(f"Failed to process {event_type} batch: {e}")
            self.metrics.errors += 1
        finally:
            session.close()

    def add_event(self, event: Dict):
        """Add an event to the appropriate queue for batch processing."""
        event_type = event.get('type')
        if event_type in self.EVENT_ENTITY_MAP:
            try:
                self.event_queues[event_type].put(event)
                
                # Process queue if it's full
                if self.event_queues[event_type].qsize() >= self.batch_size:
                    self._process_queue(event_type)
                    
            except queue.Full:
                logger.warning(f"Queue full for {event_type}, processing immediately")
                self._process_queue(event_type)
                self.event_queues[event_type].put(event)

    def _process_queue(self, event_type: str):
        """Process all events in a queue."""
        events = []
        try:
            while not self.event_queues[event_type].empty():
                events.append(self.event_queues[event_type].get_nowait())
        except queue.Empty:
            pass
        
        if events:
            self.process_event_batch(events)

    def flush_queues(self):
        """Process all remaining events in queues."""
        for event_type in self.EVENT_ENTITY_MAP:
            self._process_queue(event_type)

    def get_metrics(self) -> BatchMetrics:
        """Get current processing metrics."""
        return self.metrics

    def _get_memory_usage(self) -> float:
        """Get current memory usage in MB."""
        import psutil
        process = psutil.Process(os.getpid())
        return process.memory_info().rss / 1024 / 1024

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit with cleanup."""
        self.flush_queues()
        self.executor.shutdown(wait=True)
