"""BigQuery client for GitHub Archive data with optimized queries."""

from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
import logging
import json

from google.cloud import bigquery
from google.api_core import retry, exceptions
from google.oauth2 import service_account

from ..config import BigQueryConfig

logger = logging.getLogger(__name__)

class BigQueryClient:
    """BigQuery client for GitHub Archive data with optimized queries."""
    
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
            
            # Minimal validation of connection
            try:
                datasets = list(self.client.list_datasets())
                logger.info(f"Successfully connected to BigQuery. Found {len(datasets)} datasets.")
            except Exception as e:
                logger.error(f"Error listing datasets: {e}")
                
        except Exception as e:
            logger.error(f"Error initializing BigQuery client: {e}")
            raise
        
    def _execute_query(self, query: str, job_config: bigquery.QueryJobConfig = None) -> List[Dict[str, Any]]:
        """Execute BigQuery query with retries and error handling."""
        if job_config is None:
            job_config = bigquery.QueryJobConfig(
                maximum_bytes_billed=self.config.max_bytes_billed
            )
        
        try:
            logger.info(f"Executing query with max_bytes={job_config.maximum_bytes_billed}")
            query_job = self.client.query(
                query,
                job_config=job_config,
                retry=retry.Retry(deadline=300)
            )
            logger.info(f"Successfully started query job: {query_job.job_id}")
            
            results = [dict(row) for row in query_job]
            logger.info(f"Query returned {len(results)} results")
            return results
            
        except exceptions.BadRequest as e:
            if "Query exceeded limit for bytes billed" in str(e):
                required_bytes = self._extract_required_bytes(str(e))
                logger.error(f"Query exceeded limit for bytes billed: {job_config.maximum_bytes_billed}. Required: {required_bytes}")
                raise ValueError(f"Query exceeded byte limit. Required: {required_bytes}, Current limit: {job_config.maximum_bytes_billed}")
            logger.error(f"Bad request error executing query: {e}")
            raise
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise
    
    def _extract_required_bytes(self, error_message: str) -> int:
        """Extract required bytes from error message."""
        try:
            # Beispiel: "Query exceeded limit for bytes billed: 1000000000. 2386558976 or higher required."
            parts = error_message.split()
            for i, part in enumerate(parts):
                if part.isdigit() and i < len(parts) - 1 and parts[i+1] == "or":
                    return int(part)
            return 0
        except Exception:
            return 0
        
    def query(self, query: str, max_bytes: Optional[int] = None, retry_with_reduced_scope: bool = True) -> List[Dict[str, Any]]:
        """Execute a query with a specific maximum bytes billed and retry strategy."""
        job_config = bigquery.QueryJobConfig(
            maximum_bytes_billed=max_bytes or self.config.max_bytes_billed
        )
        
        try:
            return self._execute_query(query, job_config)
        except ValueError as e:
            if "Query exceeded byte limit" in str(e) and retry_with_reduced_scope:
                logger.warning("Query exceeded byte limit. Trying to reduce query scope...")
                # Versuche, die Abfrage zu optimieren, indem du weniger Spalten auswählst
                if "SELECT *" in query:
                    optimized_query = query.replace("SELECT *", "SELECT id, type, created_at")
                    logger.info("Retrying with fewer columns")
                    return self.query(optimized_query, max_bytes, False)
                
                # Oder versuche, den Zeitraum zu reduzieren
                if "BETWEEN" in query and not retry_with_reduced_scope:
                    logger.info("Retrying with reduced time range not possible - already tried optimizing")
                    raise
            logger.error(f"Error executing query: {e}")
            raise
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise
    
    def get_repository_metrics(self, full_name: str, since: datetime, max_bytes: Optional[int] = None) -> Dict[str, int]:
        """Get repository metrics from GitHub Archive with optimized query."""
        # Begrenze den Zeitraum auf einen Tag für Performance und Kosteneffizienz
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=1)  
        
        # Verwende _TABLE_SUFFIX für bessere Partitionsfilterung
        start_suffix = start_date.strftime("%Y%m%d")
        end_suffix = end_date.strftime("%Y%m%d")
        
        # Optimierte Abfrage mit minimalen Spalten und effizienter Filterung
        query = f"""
        SELECT
            COUNT(DISTINCT CASE WHEN type = 'WatchEvent' THEN actor.login END) as stars,
            COUNT(DISTINCT CASE WHEN type = 'PushEvent' THEN actor.login END) as contributors,
            COUNT(DISTINCT CASE WHEN type = 'PushEvent' THEN id END) as commits
        FROM `{self.config.project_id}.{self.config.dataset_id}.{self.config.table_id}`
        WHERE _TABLE_SUFFIX BETWEEN '{start_suffix}' AND '{end_suffix}'
        AND repo.name = '{full_name}'
        AND created_at BETWEEN TIMESTAMP('{start_date.isoformat()}') AND TIMESTAMP('{end_date.isoformat()}')
        """
        
        try:
            results = self.query(query, max_bytes=max_bytes)
            if not results:
                return {'stars': 0, 'contributors': 0, 'commits': 0}
            
            return {
                'stars': results[0].get('stars', 0) or 0,
                'contributors': results[0].get('contributors', 0) or 0,
                'commits': results[0].get('commits', 0) or 0
            }
        except Exception as e:
            logger.error(f"Error getting repository metrics for {full_name}: {e}")
            
            # Versuche mit noch kürzerem Zeitraum (12 Stunden)
            try:
                logger.info("Retrying with reduced time range (12 hours)")
                start_date = end_date - timedelta(hours=12)
                start_suffix = start_date.strftime("%Y%m%d")
                
                query = f"""
                SELECT
                    COUNT(DISTINCT CASE WHEN type = 'WatchEvent' THEN actor.login END) as stars,
                    COUNT(DISTINCT CASE WHEN type = 'PushEvent' THEN actor.login END) as contributors,
                    COUNT(DISTINCT CASE WHEN type = 'PushEvent' THEN id END) as commits
                FROM `{self.config.project_id}.{self.config.dataset_id}.{self.config.table_id}`
                WHERE _TABLE_SUFFIX BETWEEN '{start_suffix}' AND '{end_suffix}'
                AND repo.name = '{full_name}'
                AND created_at BETWEEN TIMESTAMP('{start_date.isoformat()}') AND TIMESTAMP('{end_date.isoformat()}')
                """
                
                results = self.query(query, max_bytes=max_bytes)
                if not results:
                    return {'stars': 0, 'contributors': 0, 'commits': 0}
                
                return {
                    'stars': results[0].get('stars', 0) or 0,
                    'contributors': results[0].get('contributors', 0) or 0,
                    'commits': results[0].get('commits', 0) or 0
                }
            except Exception as e2:
                logger.error(f"Error retrying with reduced time range: {e2}")
                return {'stars': 0, 'contributors': 0, 'commits': 0}
    
    def get_repository_events(
        self,
        full_name: str,
        event_types: Optional[List[str]] = None,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
        batch_size: Optional[int] = None,
        max_bytes: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Get repository events from GitHub Archive with optimized query."""
        if not since:
            since = datetime.now(timezone.utc) - timedelta(days=1)  
        if not until:
            until = datetime.now(timezone.utc)
        if not batch_size:
            batch_size = 100  
        
        # Begrenze den Zeitraum auf einen Tag für Performance und Kosteneffizienz
        if (until - since).days > 1:
            logger.warning(f"Limiting time range to 1 day for cost efficiency (was {(until - since).days} days)")
            since = until - timedelta(days=1)
        
        # Erstelle eine effizientere Abfrage
        event_filter = ""
        if event_types:
            event_types_str = ", ".join([f"'{t}'" for t in event_types])
            event_filter = f"AND type IN ({event_types_str})"
        
        # Verwende _TABLE_SUFFIX für bessere Partitionsfilterung
        start_suffix = since.strftime("%Y%m%d")
        end_suffix = until.strftime("%Y%m%d")
        
        # Optimierte Abfrage mit minimalen Spalten
        query = f"""
        SELECT
            id,
            type,
            actor.login as actor_login,
            repo.name as repo_name,
            created_at
        FROM `{self.config.project_id}.{self.config.dataset_id}.{self.config.table_id}`
        WHERE _TABLE_SUFFIX BETWEEN '{start_suffix}' AND '{end_suffix}'
        AND repo.name = '{full_name}'
        AND created_at BETWEEN TIMESTAMP('{since.isoformat()}') AND TIMESTAMP('{until.isoformat()}')
        {event_filter}
        LIMIT {batch_size}
        """
        
        try:
            return self.query(query, max_bytes=max_bytes)
        except Exception as e:
            logger.error(f"Error getting repository events for {full_name}: {e}")
            
            # Versuche mit noch kürzerem Zeitraum (6 Stunden)
            try:
                logger.info("Retrying with reduced time range (6 hours)")
                since = until - timedelta(hours=6)
                start_suffix = since.strftime("%Y%m%d")
                
                query = f"""
                SELECT
                    id,
                    type,
                    actor.login as actor_login,
                    repo.name as repo_name,
                    created_at
                FROM `{self.config.project_id}.{self.config.dataset_id}.{self.config.table_id}`
                WHERE _TABLE_SUFFIX BETWEEN '{start_suffix}' AND '{end_suffix}'
                AND repo.name = '{full_name}'
                AND created_at BETWEEN TIMESTAMP('{since.isoformat()}') AND TIMESTAMP('{until.isoformat()}')
                {event_filter}
                LIMIT {batch_size}
                """
                
                return self.query(query, max_bytes=max_bytes)
            except Exception as e2:
                logger.error(f"Error retrying with reduced time range: {e2}")
                return []
    
    def get_aggregated_organization_stats(self, start_year: int, end_year: int, max_bytes: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Get aggregated organization statistics by year with optimized query.
        
        Args:
            start_year: Start year for aggregation
            end_year: End year for aggregation
            max_bytes: Maximum bytes billed (optional)
            
        Returns:
            List of dictionaries with aggregated statistics
        """
        # Begrenze den Zeitraum für effizientere Abfragen
        current_year = datetime.now().year
        if end_year > current_year:
            end_year = current_year
        
        # Berechne Datumsbereich für _TABLE_SUFFIX
        start_suffix = f"{start_year}0101"
        end_suffix = f"{end_year}1231"
        
        # Optimierte Abfrage mit minimalen Spalten und effizienter Filterung
        query = f"""
        SELECT
          EXTRACT(YEAR FROM created_at) AS year,
          repo.organization.login AS organization_login,
          COUNT(DISTINCT repo.id) AS number_repos,
          SUM(CASE WHEN type = 'ForkEvent' THEN 1 ELSE 0 END) AS forks,
          SUM(CASE WHEN type = 'WatchEvent' THEN 1 ELSE 0 END) AS stars,
          COUNT(DISTINCT CASE WHEN type = 'PushEvent' THEN actor.login END) AS contributors,
          COUNT(DISTINCT CASE WHEN type = 'PushEvent' THEN id END) AS number_commits
        FROM
          `{self.config.project_id}.{self.config.dataset_id}.{self.config.table_id}`
        WHERE
          _TABLE_SUFFIX BETWEEN '{start_suffix}' AND '{end_suffix}'
          AND repo.organization.login IS NOT NULL
          AND created_at BETWEEN '{start_year}-01-01' AND '{end_year}-12-31'
        GROUP BY
          year, organization_login
        ORDER BY
          year, stars DESC
        """
        
        try:
            # Verwende einen höheren max_bytes-Wert, wenn angegeben
            return self.query(query, max_bytes=max_bytes)
        except Exception as e:
            logger.error(f"Error getting aggregated organization stats: {e}")
            
            # Bei Fehler versuche mit kürzerem Zeitraum
            if end_year - start_year > 1:
                mid_year = start_year + (end_year - start_year) // 2
                logger.info(f"Retrying with shorter time range: splitting {start_year}-{end_year} into two parts")
                
                # Rekursiv beide Hälften abrufen und kombinieren
                first_half = self.get_aggregated_organization_stats(start_year, mid_year, max_bytes)
                second_half = self.get_aggregated_organization_stats(mid_year + 1, end_year, max_bytes)
                
                return first_half + second_half
            
            # Wenn wir bereits bei einem einzelnen Jahr sind, versuche mit weniger Metriken
            logger.info(f"Retrying with fewer metrics for year {start_year}")
            simplified_query = f"""
            SELECT
              EXTRACT(YEAR FROM created_at) AS year,
              repo.organization.login AS organization_login,
              COUNT(DISTINCT repo.id) AS number_repos,
              SUM(CASE WHEN type = 'WatchEvent' THEN 1 ELSE 0 END) AS stars
            FROM
              `{self.config.project_id}.{self.config.dataset_id}.{self.config.table_id}`
            WHERE
              _TABLE_SUFFIX BETWEEN '{start_suffix}' AND '{start_suffix}'
              AND repo.organization.login IS NOT NULL
              AND created_at BETWEEN '{start_year}-01-01' AND '{start_year}-12-31'
            GROUP BY
              year, organization_login
            ORDER BY
              year, stars DESC
            """
            
            try:
                return self.query(simplified_query, max_bytes=max_bytes)
            except Exception as e2:
                logger.error(f"Error with simplified query: {e2}")
                return []
    
    def get_aggregated_country_stats(self, start_year: int, end_year: int, max_bytes: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Get aggregated country statistics by year with optimized query.
        
        Args:
            start_year: Start year for aggregation
            end_year: End year for aggregation
            max_bytes: Maximum bytes billed (optional)
            
        Returns:
            List of dictionaries with aggregated statistics
        """
        # Begrenze den Zeitraum für effizientere Abfragen
        current_year = datetime.now().year
        if end_year > current_year:
            end_year = current_year
        
        # Berechne Datumsbereich für _TABLE_SUFFIX
        start_suffix = f"{start_year}0101"
        end_suffix = f"{end_year}1231"
        
        # Optimierte Abfrage mit minimalen Spalten und effizienter Filterung
        # Da wir die Länderinformationen später hinzufügen, fokussieren wir uns hier auf die Organisationen
        query = f"""
        SELECT
          EXTRACT(YEAR FROM created_at) AS year,
          repo.organization.login AS organization_login,
          COUNT(DISTINCT repo.id) AS number_repos,
          SUM(CASE WHEN type = 'ForkEvent' THEN 1 ELSE 0 END) AS forks,
          SUM(CASE WHEN type = 'WatchEvent' THEN 1 ELSE 0 END) AS stars,
          COUNT(DISTINCT CASE WHEN type = 'PushEvent' THEN actor.login END) AS contributors,
          COUNT(DISTINCT CASE WHEN type = 'PushEvent' THEN id END) AS number_commits
        FROM
          `{self.config.project_id}.{self.config.dataset_id}.{self.config.table_id}`
        WHERE
          _TABLE_SUFFIX BETWEEN '{start_suffix}' AND '{end_suffix}'
          AND repo.organization.login IS NOT NULL
          AND created_at BETWEEN '{start_year}-01-01' AND '{end_year}-12-31'
        GROUP BY
          year, organization_login
        ORDER BY
          year, stars DESC
        """
        
        try:
            # Verwende einen höheren max_bytes-Wert, wenn angegeben
            return self.query(query, max_bytes=max_bytes)
        except Exception as e:
            logger.error(f"Error getting aggregated country stats: {e}")
            
            # Bei Fehler versuche mit kürzerem Zeitraum
            if end_year - start_year > 1:
                mid_year = start_year + (end_year - start_year) // 2
                logger.info(f"Retrying with shorter time range: splitting {start_year}-{end_year} into two parts")
                
                # Rekursiv beide Hälften abrufen und kombinieren
                first_half = self.get_aggregated_country_stats(start_year, mid_year, max_bytes)
                second_half = self.get_aggregated_country_stats(mid_year + 1, end_year, max_bytes)
                
                return first_half + second_half
            
            # Wenn wir bereits bei einem einzelnen Jahr sind, versuche mit weniger Metriken
            logger.info(f"Retrying with fewer metrics for year {start_year}")
            simplified_query = f"""
            SELECT
              EXTRACT(YEAR FROM created_at) AS year,
              repo.organization.login AS organization_login,
              COUNT(DISTINCT repo.id) AS number_repos,
              SUM(CASE WHEN type = 'WatchEvent' THEN 1 ELSE 0 END) AS stars
            FROM
              `{self.config.project_id}.{self.config.dataset_id}.{self.config.table_id}`
            WHERE
              _TABLE_SUFFIX BETWEEN '{start_suffix}' AND '{start_suffix}'
              AND repo.organization.login IS NOT NULL
              AND created_at BETWEEN '{start_year}-01-01' AND '{start_year}-12-31'
            GROUP BY
              year, organization_login
            ORDER BY
              year, stars DESC
            """
            
            try:
                return self.query(simplified_query, max_bytes=max_bytes)
            except Exception as e2:
                logger.error(f"Error with simplified query: {e2}")
                return []
