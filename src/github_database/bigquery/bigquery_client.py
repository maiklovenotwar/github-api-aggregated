"""BigQuery client for GitHub Archive data."""

import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any, Tuple

from google.cloud import bigquery
from google.api_core import retry

from ..config import BigQueryConfig

logger = logging.getLogger(__name__)

class BigQueryClient:
    """Client for querying GitHub Archive data in BigQuery."""
    
    def __init__(self, config: BigQueryConfig):
        """
        Initialize BigQuery client.
        
        Args:
            config: BigQuery configuration
        """
        self.config = config
        self.client = bigquery.Client(
            project=config.project_id,
            location="US"  # GitHub Archive is in US multi-region
        )
        
        # Set default query job configuration
        self.default_job_config = bigquery.QueryJobConfig(
            maximum_bytes_billed=config.max_bytes_billed
        )
        
        # Create dataset if it doesn't exist
        dataset_ref = self.client.dataset(config.dataset_id)
        try:
            self.client.get_dataset(dataset_ref)
        except Exception:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"
            self.client.create_dataset(dataset)
            
        # Create table if it doesn't exist
        table_ref = dataset_ref.table(config.table_id)
        try:
            self.client.get_table(table_ref)
        except Exception:
            schema = [
                bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("type", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("actor", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("repo", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("payload", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("public", "BOOLEAN", mode="REQUIRED"),
                bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("org", "STRING"),
                bigquery.SchemaField("other", "STRING"),
            ]
            
            table = bigquery.Table(table_ref, schema=schema)
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="created_at",
            )
            self.client.create_table(table)
            
    def _build_date_partitions(self, start_date: datetime, end_date: datetime) -> List[str]:
        """
        Build list of date partitions to query.
        
        Args:
            start_date: Start date
            end_date: End date
            
        Returns:
            List of partition strings in format YYYYMMDD
        """
        partitions = []
        current_date = start_date
        
        while current_date <= end_date:
            partitions.append(current_date.strftime("%Y%m%d"))
            current_date += timedelta(days=1)
            
        return partitions
        
    def _execute_query(
        self,
        query: str,
        params: Optional[List[bigquery.ScalarQueryParameter]] = None,
        retry_count: int = 3
    ) -> List[Dict]:
        """
        Execute BigQuery query with retries.
        
        Args:
            query: SQL query
            params: Query parameters
            retry_count: Number of retries
            
        Returns:
            List of result rows as dictionaries
        """
        # Use default job config and update with parameters
        job_config = self.default_job_config
        if params:
            job_config.query_parameters = params
        
        try:
            query_job = self.client.query(
                query,
                job_config=job_config,
                retry=retry.Retry(deadline=30)
            )
            results = query_job.result()
            return [dict(row.items()) for row in results]
            
        except Exception as e:
            logger.error(f"BigQuery error: {e}")
            raise
            
    def get_events(
        self,
        start_date: datetime,
        end_date: datetime,
        repository_ids: Optional[List[int]] = None,
        event_types: Optional[List[str]] = None
    ) -> List[Dict]:
        """
        Get GitHub events from specified time range.
        
        Args:
            start_date: Start date
            end_date: End date
            repository_ids: Optional list of repository IDs to filter
            event_types: Optional list of event types to filter
            
        Returns:
            List of events
        """
        # Format dates for table name
        current_date = start_date
        table_dates = []
        while current_date <= end_date:
            table_dates.append(current_date.strftime("%Y%m%d"))
            current_date += timedelta(days=1)
            
        # Build UNION ALL query for each date
        queries = []
        for date in table_dates:
            query = f"""
            SELECT
                type,
                repo.id as id,
                repo.name as name,
                actor.id as actor_id,
                actor.login as actor_login,
                created_at,
                STRUCT(
                    CASE
                        WHEN type = 'PushEvent' THEN STRUCT(
                            JSON_EXTRACT_SCALAR(payload, '$.ref') as ref,
                            ARRAY(
                                SELECT AS STRUCT 
                                    JSON_EXTRACT_SCALAR(commit, '$.sha') as sha,
                                    JSON_EXTRACT_SCALAR(commit, '$.message') as message,
                                    JSON_EXTRACT_SCALAR(commit, '$.author.name') as author_name,
                                    JSON_EXTRACT_SCALAR(commit, '$.author.email') as author_email
                                FROM UNNEST(JSON_EXTRACT_ARRAY(payload, '$.commits')) as commit
                            ) as commits
                        )
                        ELSE NULL
                    END as push,
                    CASE
                        WHEN type = 'PullRequestEvent' THEN STRUCT(
                            JSON_EXTRACT_SCALAR(payload, '$.action') as action,
                            STRUCT(
                                CAST(JSON_EXTRACT_SCALAR(payload, '$.pull_request.number') as INT64) as number,
                                JSON_EXTRACT_SCALAR(payload, '$.pull_request.title') as title,
                                JSON_EXTRACT_SCALAR(payload, '$.pull_request.body') as body,
                                JSON_EXTRACT_SCALAR(payload, '$.pull_request.state') as state
                            ) as pull_request
                        )
                        ELSE NULL
                    END as pull_request,
                    CASE
                        WHEN type = 'IssuesEvent' THEN STRUCT(
                            JSON_EXTRACT_SCALAR(payload, '$.action') as action,
                            STRUCT(
                                CAST(JSON_EXTRACT_SCALAR(payload, '$.issue.number') as INT64) as number,
                                JSON_EXTRACT_SCALAR(payload, '$.issue.title') as title,
                                JSON_EXTRACT_SCALAR(payload, '$.issue.body') as body,
                                JSON_EXTRACT_SCALAR(payload, '$.issue.state') as state
                            ) as issue
                        )
                        ELSE NULL
                    END as issue,
                    CASE
                        WHEN type = 'ForkEvent' THEN STRUCT(
                            CAST(JSON_EXTRACT_SCALAR(payload, '$.forkee.id') as INT64) as id,
                            JSON_EXTRACT_SCALAR(payload, '$.forkee.full_name') as full_name
                        )
                        ELSE NULL
                    END as forkee,
                    CASE
                        WHEN type = 'WatchEvent' THEN STRUCT(
                            JSON_EXTRACT_SCALAR(payload, '$.action') as action
                        )
                        ELSE NULL
                    END as watch
                ) as payload
            FROM `githubarchive.day.{date}`
            """
            
            conditions = []
            if repository_ids:
                repo_ids_str = ", ".join(str(id) for id in repository_ids)
                conditions.append(f"repo.id IN ({repo_ids_str})")
                
            if event_types:
                event_types_str = ", ".join(f"'{type}'" for type in event_types)
                conditions.append(f"type IN ({event_types_str})")
                
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
                
            queries.append(query)
            
        final_query = " UNION ALL ".join(queries)
        logger.info(f"Executing query: {final_query}")
        return self._execute_query(final_query)
        
    def get_repository_metrics(
        self,
        repository_ids: List[int]
    ) -> List[Dict]:
        """
        Get repository metrics from GitHub Archive.
        
        Args:
            repository_ids: List of repository IDs
            
        Returns:
            List of repository metrics
        """
        repo_ids_str = ", ".join(str(id) for id in repository_ids)
        
        # Calculate date for 1 year ago
        one_year_ago = (datetime.now() - timedelta(days=365)).strftime("%Y%m%d")
        today = datetime.now().strftime("%Y%m%d")
        
        # Build UNION ALL query for each date
        query = f"""
        WITH events AS (
            SELECT
                repo.id as repo_id,
                type
            FROM `githubarchive.day.{one_year_ago}`
            WHERE repo.id IN ({repo_ids_str})
            UNION ALL
            SELECT
                repo.id as repo_id,
                type
            FROM `githubarchive.day.{today}`
            WHERE repo.id IN ({repo_ids_str})
        )
        SELECT
            repo_id,
            COUNT(*) as total_events,
            COUNTIF(type = 'WatchEvent') as watch_count,
            COUNTIF(type = 'ForkEvent') as fork_count,
            COUNTIF(type = 'PushEvent') as push_count
        FROM events
        GROUP BY repo_id
        """
        
        return self._execute_query(query)
        
    def get_event_distribution(
        self,
        start_date: datetime,
        end_date: datetime,
        repository_ids: Optional[List[int]] = None
    ) -> List[Dict]:
        """
        Get event type distribution for repositories.
        
        Args:
            start_date: Start date
            end_date: End date
            repository_ids: Optional list of repository IDs to filter
            
        Returns:
            List of event type distributions
        """
        partitions = self._build_date_partitions(start_date, end_date)
        
        query = f"""
        SELECT
            type as event_type,
            COUNT(*) as count,
            COUNT(DISTINCT JSON_EXTRACT_SCALAR(repo, '$.id')) as repo_count,
            COUNT(DISTINCT JSON_EXTRACT_SCALAR(actor, '$.id')) as actor_count
        FROM `{self.config.full_table_id}`
        WHERE _TABLE_SUFFIX IN UNNEST(@partitions)
        """
        
        params = [
            bigquery.ArrayQueryParameter("partitions", "STRING", partitions)
        ]
        
        if repository_ids:
            repo_ids_str = ", ".join(f"'{id}'" for id in repository_ids)  # Convert to string literals
            query += f" AND JSON_EXTRACT_SCALAR(repo, '$.id') IN ({repo_ids_str})"
            
        query += """
        GROUP BY type
        ORDER BY count DESC
        """
        
        return self._execute_query(query, params)
