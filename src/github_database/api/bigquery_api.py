"""BigQuery client for GitHub Archive data."""

from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
import logging
import json

from google.cloud import bigquery
from google.api_core import retry
from google.oauth2 import service_account

from ..config import BigQueryConfig

logger = logging.getLogger(__name__)

class BigQueryClient:
    """BigQuery client for GitHub Archive data."""
    
    def __init__(self, config: BigQueryConfig):
        """Initialize BigQuery client."""
        self.config = config
        
        # Get credentials
        credentials = None
        if config.credentials_path and config.credentials_path.exists():
            try:
                credentials = service_account.Credentials.from_service_account_file(
                    str(config.credentials_path),
                    scopes=["https://www.googleapis.com/auth/bigquery"]
                )
                logger.info("Successfully loaded BigQuery credentials")
            except Exception as e:
                logger.error(f"Error loading BigQuery credentials: {e}")
                raise
        
        # Initialize client
        try:
            self.client = bigquery.Client(
                project=config.project_id,
                credentials=credentials,
                location="US"
            )
            logger.info("Successfully initialized BigQuery client")
            
            # List available datasets
            try:
                datasets = list(self.client.list_datasets())
                logger.info(f"Available datasets in project {config.project_id}:")
                for dataset in datasets:
                    logger.info(f"- {dataset.dataset_id}")
                    
                    # List tables in dataset
                    try:
                        tables = list(self.client.list_tables(dataset))
                        logger.info(f"Tables in {dataset.dataset_id}:")
                        for table in tables:
                            logger.info(f"  - {table.table_id}")
                            
                            # Get table schema
                            table_ref = self.client.get_table(table)
                            logger.info(f"Schema for {table.table_id}:")
                            for field in table_ref.schema:
                                logger.info(f"    - {field.name}: {field.field_type}")
                                
                    except Exception as e:
                        logger.error(f"Error listing tables in {dataset.dataset_id}: {e}")
                        
            except Exception as e:
                logger.error(f"Error listing datasets: {e}")
                
            # Check data in tables
            try:
                logger.info("Checking public GitHub Archive data...")
                query = f"""
                SELECT 
                    MIN(created_at) as min_date,
                    MAX(created_at) as max_date,
                    COUNT(*) as total_rows
                FROM `githubarchive.day.20240321`
                LIMIT 1000
                """
                results = self._execute_query(query)
                if results:
                    logger.info(f"Public GitHub Archive stats: {results[0]}")
                    
                # Check event types
                query = f"""
                SELECT 
                    type, 
                    COUNT(*) as count
                FROM `githubarchive.day.20240321`
                GROUP BY type
                ORDER BY count DESC
                LIMIT 10
                """
                results = self._execute_query(query)
                if results:
                    logger.info("Event types in public GitHub Archive:")
                    for row in results:
                        logger.info(f"  - {row['type']}: {row['count']}")
                        
                # Check sample repo data
                query = f"""
                SELECT 
                    repo.name as repo_name,
                    COUNT(*) as event_count
                FROM `githubarchive.day.20240321`
                GROUP BY repo_name
                ORDER BY event_count DESC
                LIMIT 10
                """
                results = self._execute_query(query)
                if results:
                    logger.info("Top repositories in public GitHub Archive:")
                    for row in results:
                        logger.info(f"  - {row['repo_name']}: {row['event_count']}")
                
            except Exception as e:
                logger.error(f"Error checking public GitHub Archive data: {e}")
                
        except Exception as e:
            logger.error(f"Error initializing BigQuery client: {e}")
            raise
        
    def _execute_query(self, query: str, job_config: bigquery.QueryJobConfig = None) -> List[Dict[str, Any]]:
        """Execute BigQuery query with retries."""
        if job_config is None:
            job_config = bigquery.QueryJobConfig(
                maximum_bytes_billed=self.config.max_bytes_billed
            )
        
        try:
            query_job = self.client.query(
                query,
                job_config=job_config,
                retry=retry.Retry(deadline=300)
            )
            logger.info(f"Successfully started query job: {query_job.job_id}")
            
            results = [dict(row) for row in query_job]
            logger.info(f"Query returned {len(results)} results")
            return results
            
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise
        
    def get_repository_metrics(self, full_name: str, since: datetime) -> Dict[str, int]:
        """Get repository metrics from GitHub Archive."""
        # Begrenze den Zeitraum auf einen Tag für Performance und Kosteneffizienz
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=1)  
        
        # Erstelle eine effizientere Abfrage für die 'day' Tabelle
        query = f"""
        SELECT
            COUNT(DISTINCT CASE WHEN type = 'WatchEvent' THEN JSON_EXTRACT_SCALAR(actor, '$.login') END) as stars,
            COUNT(DISTINCT CASE WHEN type = 'PushEvent' THEN JSON_EXTRACT_SCALAR(actor, '$.login') END) as contributors,
            COUNT(DISTINCT CASE WHEN type = 'PushEvent' THEN id END) as commits
        FROM `{self.config.project_id}.{self.config.dataset_id}.day`
        WHERE created_at BETWEEN TIMESTAMP('{start_date.isoformat()}') AND TIMESTAMP('{end_date.isoformat()}')
        AND JSON_EXTRACT_SCALAR(repo, '$.name') = '{full_name}'
        """
        
        try:
            job_config = bigquery.QueryJobConfig(
                maximum_bytes_billed=self.config.max_bytes_billed
            )
            
            results = self._execute_query(query, job_config)
            if not results:
                return {'stars': 0, 'contributors': 0, 'commits': 0}
            
            return {
                'stars': results[0].get('stars', 0),
                'contributors': results[0].get('contributors', 0),
                'commits': results[0].get('commits', 0)
            }
            
        except Exception as e:
            logger.error(f"Error getting repository metrics for {full_name}: {e}")
            return {'stars': 0, 'contributors': 0, 'commits': 0}
        
    def get_repository_events(
        self,
        full_name: str,
        event_types: Optional[List[str]] = None,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
        batch_size: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Get repository events from GitHub Archive."""
        if not since:
            since = datetime.now(timezone.utc) - timedelta(days=1)  
        if not until:
            until = datetime.now(timezone.utc)
        if not batch_size:
            batch_size = 100  
        
        # Begrenze den Zeitraum auf einen Tag für Performance und Kosteneffizienz
        # Dies entspricht dem BIGQUERY_MAX_BYTES Limit von 1GB
        if (until - since).days > 1:
            logger.warning(f"Limiting query timeframe to 1 day instead of {(until - since).days} days to respect query limits")
            since = until - timedelta(days=1)
            
        # Erstelle eine effizientere Abfrage mit begrenzten Spalten
        type_filter = ""
        if event_types and len(event_types) > 0:
            type_filter = f"AND type IN ({', '.join(['%s' % t for t in event_types])})"
            
        query = f"""
        SELECT
            id,
            type,
            created_at,
            JSON_EXTRACT(payload, '$') as payload,
            JSON_EXTRACT_SCALAR(actor, '$.login') as actor_login
        FROM `{self.config.project_id}.{self.config.dataset_id}.day`
        WHERE created_at BETWEEN TIMESTAMP('{since.isoformat()}') AND TIMESTAMP('{until.isoformat()}')
        AND JSON_EXTRACT_SCALAR(repo, '$.name') = '{full_name}'
        {type_filter}
        ORDER BY created_at DESC
        LIMIT {batch_size}
        """
        
        try:
            job_config = bigquery.QueryJobConfig(
                maximum_bytes_billed=self.config.max_bytes_billed
            )
            
            events = self._execute_query(query, job_config)
            
            # Konvertiere die Payload von String zu Dict
            for event in events:
                try:
                    if 'payload' in event and event['payload']:
                        event['payload'] = json.loads(event['payload'])
                except (json.JSONDecodeError, TypeError) as e:
                    logger.warning(f"Error parsing event payload: {e}")
            
            return events
            
        except Exception as e:
            logger.error(f"Error getting repository events for {full_name}: {e}")
            return []
        
    def get_trending_repositories(
        self,
        since: datetime,
        min_stars: int = 10,  
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get trending repositories from GitHub Archive."""
        # Begrenze den Zeitraum auf die letzten 3 Tage für Performance und Kosteneffizienz
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=3)
        if start_date < since:
            start_date = since
        
        # Erstelle eine effizientere Abfrage für die 'day' Tabelle
        query = f"""
        SELECT
            JSON_EXTRACT_SCALAR(repo, '$.name') as full_name,
            COUNT(DISTINCT CASE WHEN type = 'WatchEvent' THEN JSON_EXTRACT_SCALAR(actor, '$.login') END) as stars,
            COUNT(DISTINCT CASE WHEN type = 'PushEvent' THEN JSON_EXTRACT_SCALAR(actor, '$.login') END) as contributors,
            COUNT(CASE WHEN type = 'PushEvent' THEN 1 END) as commits
        FROM `{self.config.project_id}.{self.config.dataset_id}.day`
        WHERE created_at BETWEEN TIMESTAMP('{start_date.isoformat()}') AND TIMESTAMP('{end_date.isoformat()}')
        GROUP BY full_name
        HAVING stars >= {min_stars}
        ORDER BY stars DESC, commits DESC
        LIMIT {limit}
        """
        
        try:
            job_config = bigquery.QueryJobConfig(
                maximum_bytes_billed=self.config.max_bytes_billed
            )
            
            results = self._execute_query(query, job_config)
            return results
        
        except Exception as e:
            logger.error(f"Error getting trending repositories: {e}")
            return []
