"""SQL query builder for GitHub Archive BigQuery data."""

from datetime import datetime
from typing import List, Optional, Dict, Any

class GitHubArchiveQueryBuilder:
    """Build optimized BigQuery queries for GitHub Archive data."""
    
    @staticmethod
    def build_events_query(
        table_id: str,
        repo_ids: List[int],
        start_date: datetime,
        end_date: datetime,
        event_types: Optional[List[str]] = None,
        actor_ids: Optional[List[int]] = None,
        include_payload: bool = True
    ) -> Dict[str, Any]:
        """
        Build a query for fetching events with parameters.
        
        Args:
            table_id: Full BigQuery table ID
            repo_ids: List of repository IDs to query
            start_date: Start date for events
            end_date: End date for events
            event_types: Optional list of event types to filter
            actor_ids: Optional list of actor IDs to filter
            include_payload: Whether to include full event payload
            
        Returns:
            Dict containing query string and parameters
        """
        select_fields = [
            "repo.id as repo_id",
            "type as event_type",
            "actor.id as actor_id",
            "actor.login as actor_login",
            "created_at"
        ]
        
        if include_payload:
            select_fields.append("payload")
            
        query = f"""
        SELECT {', '.join(select_fields)}
        FROM `{table_id}`
        WHERE repo.id IN UNNEST(@repo_ids)
        AND created_at BETWEEN @start_date AND @end_date
        """
        
        params = [
            {"name": "repo_ids", "type": "INT64", "value": repo_ids},
            {"name": "start_date", "type": "TIMESTAMP", "value": start_date},
            {"name": "end_date", "type": "TIMESTAMP", "value": end_date}
        ]
        
        if event_types:
            query += " AND type IN UNNEST(@event_types)"
            params.append({
                "name": "event_types",
                "type": "STRING",
                "value": event_types
            })
            
        if actor_ids:
            query += " AND actor.id IN UNNEST(@actor_ids)"
            params.append({
                "name": "actor_ids",
                "type": "INT64",
                "value": actor_ids
            })
            
        query += " ORDER BY created_at"
        
        return {
            "query": query,
            "params": params
        }
        
    @staticmethod
    def build_metrics_query(
        table_id: str,
        repo_ids: List[int],
        time_window_days: int = 365
    ) -> Dict[str, Any]:
        """
        Build a query for fetching repository metrics.
        
        Args:
            table_id: Full BigQuery table ID
            repo_ids: List of repository IDs
            time_window_days: Number of days to look back for metrics
            
        Returns:
            Dict containing query string and parameters
        """
        query = f"""
        WITH events AS (
            SELECT
                repo.id as repo_id,
                type as event_type,
                created_at,
                actor.id as actor_id
            FROM `{table_id}`
            WHERE repo.id IN UNNEST(@repo_ids)
            AND created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @days DAY)
        ),
        commit_stats AS (
            SELECT
                repo_id,
                COUNT(DISTINCT actor_id) as unique_contributors,
                COUNT(*) as total_commits
            FROM events
            WHERE event_type = 'PushEvent'
            GROUP BY repo_id
        ),
        engagement_stats AS (
            SELECT
                repo_id,
                COUNTIF(event_type = 'WatchEvent') as stars,
                COUNTIF(event_type = 'ForkEvent') as forks,
                COUNTIF(event_type = 'IssuesEvent') as issues,
                COUNTIF(event_type = 'PullRequestEvent') as pull_requests
            FROM events
            GROUP BY repo_id
        )
        SELECT
            c.repo_id,
            c.unique_contributors,
            c.total_commits,
            e.stars,
            e.forks,
            e.issues,
            e.pull_requests
        FROM commit_stats c
        JOIN engagement_stats e ON c.repo_id = e.repo_id
        """
        
        params = [
            {"name": "repo_ids", "type": "INT64", "value": repo_ids},
            {"name": "days", "type": "INT64", "value": time_window_days}
        ]
        
        return {
            "query": query,
            "params": params
        }
        
    @staticmethod
    def build_contributor_query(
        table_id: str,
        repo_ids: List[int],
        min_contributions: int = 1,
        time_window_days: int = 365
    ) -> Dict[str, Any]:
        """
        Build a query for fetching repository contributors.
        
        Args:
            table_id: Full BigQuery table ID
            repo_ids: List of repository IDs
            min_contributions: Minimum number of contributions to be included
            time_window_days: Number of days to look back
            
        Returns:
            Dict containing query string and parameters
        """
        query = f"""
        WITH contributor_events AS (
            SELECT
                repo.id as repo_id,
                actor.id as actor_id,
                actor.login as actor_login,
                type as event_type,
                COUNT(*) as event_count
            FROM `{table_id}`
            WHERE repo.id IN UNNEST(@repo_ids)
            AND created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @days DAY)
            AND type IN ('PushEvent', 'PullRequestEvent')
            GROUP BY repo_id, actor_id, actor_login, type
            HAVING event_count >= @min_contributions
        )
        SELECT
            repo_id,
            actor_id,
            actor_login,
            SUM(CASE WHEN event_type = 'PushEvent' THEN event_count ELSE 0 END) as commits,
            SUM(CASE WHEN event_type = 'PullRequestEvent' THEN event_count ELSE 0 END) as pull_requests,
            SUM(event_count) as total_contributions
        FROM contributor_events
        GROUP BY repo_id, actor_id, actor_login
        ORDER BY total_contributions DESC
        """
        
        params = [
            {"name": "repo_ids", "type": "INT64", "value": repo_ids},
            {"name": "days", "type": "INT64", "value": time_window_days},
            {"name": "min_contributions", "type": "INT64", "value": min_contributions}
        ]
        
        return {
            "query": query,
            "params": params
        }
