"""Data enrichment component for GitHub Archive events."""

import logging
import json
import time
import threading
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set, Any
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
import diskcache

from ..config import ETLConfig
from ..api.github_api import GitHubAPI

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class CacheConfig:
    """Configuration for caching behavior."""
    memory_cache_size: int = 10000  # Number of items in memory
    disk_cache_size_bytes: int = 1024 * 1024 * 100  # 100MB
    cache_ttl: int = 60 * 60 * 24 * 7  # 7 days in seconds
    batch_size: int = 100  # Number of items to process in batch

class Cache:
    """Multi-level cache implementation."""
    
    def __init__(self, config: CacheConfig, cache_dir: Path):
        self.config = config
        self._memory_cache: Dict[str, Any] = {}
        self._memory_cache_lock = threading.Lock()
        self._disk_cache = diskcache.Cache(str(cache_dir))
        self._access_counts: Dict[str, int] = {}
        
    def get(self, key: str) -> Optional[Any]:
        """Get item from cache, checking memory first then disk."""
        # Check memory cache
        with self._memory_cache_lock:
            if key in self._memory_cache:
                self._increment_access(key)
                return self._memory_cache[key]
                
        # Check disk cache
        value = self._disk_cache.get(key)
        if value is not None:
            self._promote_to_memory(key, value)
            return value
            
        return None
        
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """Set item in both memory and disk cache."""
        ttl = ttl or self.config.cache_ttl
        
        # Update disk cache
        self._disk_cache.set(key, value, expire=ttl)
        
        # Update memory cache
        self._promote_to_memory(key, value)
        
    def _promote_to_memory(self, key: str, value: Any) -> None:
        """Promote item to memory cache, managing size limits."""
        with self._memory_cache_lock:
            if len(self._memory_cache) >= self.config.memory_cache_size:
                # Remove least accessed items
                sorted_items = sorted(
                    self._access_counts.items(),
                    key=lambda x: x[1]
                )
                to_remove = sorted_items[:len(sorted_items) // 4]  # Remove 25%
                for k, _ in to_remove:
                    self._memory_cache.pop(k, None)
                    self._access_counts.pop(k, None)
                    
            self._memory_cache[key] = value
            self._increment_access(key)
            
    def _increment_access(self, key: str) -> None:
        """Track access counts for cache items."""
        self._access_counts[key] = self._access_counts.get(key, 0) + 1
        
    def invalidate(self, key: str) -> None:
        """Remove item from all cache levels."""
        with self._memory_cache_lock:
            self._memory_cache.pop(key, None)
            self._access_counts.pop(key, None)
        self._disk_cache.delete(key)
        
    def clear(self) -> None:
        """Clear all cache levels."""
        with self._memory_cache_lock:
            self._memory_cache.clear()
            self._access_counts.clear()
        self._disk_cache.clear()

class DataEnricher:
    """Enriches GitHub Archive events with additional data from GitHub API."""
    
    def __init__(self, config: ETLConfig, cache_dir: Path):
        self.config = config
        self.github_api = GitHubAPI(config.api.token)
        self.cache = Cache(CacheConfig(), cache_dir)
        self._rate_limit_lock = threading.Lock()
        self._last_api_call = 0.0
        
    def enrich_repository(self, repo_dict: Dict) -> Dict:
        """
        Enrich repository data with additional information from GitHub API.
        
        Args:
            repo_dict: Basic repository information
            
        Returns:
            Dict: Enriched repository data
        """
        repo_id = repo_dict['id']
        cache_key = f'repo:{repo_id}'
        
        # Check cache first
        cached_data = self.cache.get(cache_key)
        if cached_data:
            return cached_data
            
        try:
            # Respect API rate limits
            self._wait_for_rate_limit()
            
            # Fetch additional data
            owner, name = repo_dict['name'].split('/')
            repo_data = self.github_api.get_repository(owner, name)
            
            if repo_data:
                enriched_data = {
                    **repo_dict,
                    'description': repo_data.get('description'),
                    'language': repo_data.get('language'),
                    'stars': repo_data.get('stargazers_count', 0),
                    'forks': repo_data.get('forks_count', 0),
                    'created_at': repo_data.get('created_at'),
                    'updated_at': repo_data.get('updated_at'),
                    'topics': repo_data.get('topics', [])
                }
                
                # Cache the enriched data
                self.cache.set(cache_key, enriched_data)
                return enriched_data
                
        except Exception as e:
            logger.error(f"Error enriching repository {repo_id}: {e}")
            
        return repo_dict
        
    def enrich_user(self, user_dict: Dict) -> Dict:
        """
        Enrich user data with additional information from GitHub API.
        
        Args:
            user_dict: Basic user information
            
        Returns:
            Dict: Enriched user data
        """
        user_id = user_dict['id']
        cache_key = f'user:{user_id}'
        
        cached_data = self.cache.get(cache_key)
        if cached_data:
            return cached_data
            
        try:
            self._wait_for_rate_limit()
            
            user_data = self.github_api.get_user(user_dict['login'])
            
            if user_data:
                enriched_data = {
                    **user_dict,
                    'name': user_data.get('name'),
                    'email': user_data.get('email'),
                    'company': user_data.get('company'),
                    'location': user_data.get('location'),
                    'created_at': user_data.get('created_at'),
                    'updated_at': user_data.get('updated_at')
                }
                
                self.cache.set(cache_key, enriched_data)
                return enriched_data
                
        except Exception as e:
            logger.error(f"Error enriching user {user_id}: {e}")
            
        return user_dict
        
    def enrich_commit(self, commit_dict: Dict, repo_name: str) -> Dict:
        """
        Enrich commit data with additional information from GitHub API.
        
        Args:
            commit_dict: Basic commit information
            repo_name: Full repository name (owner/repo)
            
        Returns:
            Dict: Enriched commit data
        """
        commit_sha = commit_dict['sha']
        cache_key = f'commit:{repo_name}:{commit_sha}'
        
        cached_data = self.cache.get(cache_key)
        if cached_data:
            return cached_data
            
        try:
            self._wait_for_rate_limit()
            
            owner, repo = repo_name.split('/')
            commit_data = self.github_api.get_commit(owner, repo, commit_sha)
            
            if commit_data:
                enriched_data = {
                    **commit_dict,
                    'stats': commit_data.get('stats'),
                    'files': commit_data.get('files', []),
                    'author': commit_data.get('author', {}),
                    'committer': commit_data.get('committer', {})
                }
                
                self.cache.set(cache_key, enriched_data)
                return enriched_data
                
        except Exception as e:
            logger.error(f"Error enriching commit {commit_sha}: {e}")
            
        return commit_dict
        
    def enrich_pull_request(self, pr_dict: Dict, repo_name: str) -> Dict:
        """
        Enrich pull request data with additional information from GitHub API.
        
        Args:
            pr_dict: Basic pull request information
            repo_name: Full repository name (owner/repo)
            
        Returns:
            Dict: Enriched pull request data
        """
        pr_number = pr_dict['number']
        cache_key = f'pr:{repo_name}:{pr_number}'
        
        cached_data = self.cache.get(cache_key)
        if cached_data:
            return cached_data
            
        try:
            self._wait_for_rate_limit()
            
            owner, repo = repo_name.split('/')
            pr_data = self.github_api.get_pull_request(owner, repo, pr_number)
            
            if pr_data:
                enriched_data = {
                    **pr_dict,
                    'merged_by': pr_data.get('merged_by', {}),
                    'review_comments': pr_data.get('review_comments', 0),
                    'commits': pr_data.get('commits', 0),
                    'additions': pr_data.get('additions', 0),
                    'deletions': pr_data.get('deletions', 0),
                    'changed_files': pr_data.get('changed_files', 0)
                }
                
                self.cache.set(cache_key, enriched_data)
                return enriched_data
                
        except Exception as e:
            logger.error(f"Error enriching PR {pr_number}: {e}")
            
        return pr_dict
        
    def enrich_issue(self, issue_dict: Dict, repo_name: str) -> Dict:
        """
        Enrich issue data with additional information from GitHub API.
        
        Args:
            issue_dict: Basic issue information
            repo_name: Full repository name (owner/repo)
            
        Returns:
            Dict: Enriched issue data
        """
        issue_number = issue_dict['number']
        cache_key = f'issue:{repo_name}:{issue_number}'
        
        cached_data = self.cache.get(cache_key)
        if cached_data:
            return cached_data
            
        try:
            self._wait_for_rate_limit()
            
            owner, repo = repo_name.split('/')
            issue_data = self.github_api.get_issue(owner, repo, issue_number)
            
            if issue_data:
                enriched_data = {
                    **issue_dict,
                    'labels': issue_data.get('labels', []),
                    'assignees': issue_data.get('assignees', []),
                    'comments': issue_data.get('comments', 0),
                    'closed_by': issue_data.get('closed_by', {})
                }
                
                self.cache.set(cache_key, enriched_data)
                return enriched_data
                
        except Exception as e:
            logger.error(f"Error enriching issue {issue_number}: {e}")
            
        return issue_dict
        
    def batch_enrich_events(self, events: List[Dict]) -> List[Dict]:
        """
        Enrich multiple events in parallel, respecting rate limits.
        
        Args:
            events: List of events to enrich
            
        Returns:
            List[Dict]: List of enriched events
        """
        enriched_events = []
        
        with ThreadPoolExecutor(max_workers=self.config.api.parallel_requests) as executor:
            # Group events by type for efficient processing
            event_groups = self._group_events_by_type(events)
            
            for event_type, group_events in event_groups.items():
                # Process each batch
                for batch in self._chunk_list(group_events, self.config.database.batch_size):
                    futures = []
                    
                    for event in batch:
                        future = executor.submit(
                            self._enrich_single_event,
                            event,
                            event_type
                        )
                        futures.append(future)
                        
                    # Collect results
                    for future in futures:
                        try:
                            enriched_event = future.result()
                            if enriched_event:
                                enriched_events.append(enriched_event)
                        except Exception as e:
                            logger.error(f"Error in batch enrichment: {e}")
                            
        return enriched_events
        
    def _enrich_single_event(self, event: Dict, event_type: str) -> Optional[Dict]:
        """Enrich a single event based on its type."""
        try:
            repo_name = event['repo']['name']
            
            # Enrich repository and actor for all events
            event['repo'] = self.enrich_repository(event['repo'])
            event['actor'] = self.enrich_user(event['actor'])
            
            # Event-specific enrichment
            if event_type == 'PushEvent':
                for commit in event['payload'].get('commits', []):
                    commit.update(self.enrich_commit(commit, repo_name))
                    
            elif event_type == 'PullRequestEvent':
                pr_data = event['payload']['pull_request']
                event['payload']['pull_request'] = self.enrich_pull_request(
                    pr_data,
                    repo_name
                )
                
            elif event_type == 'IssuesEvent':
                issue_data = event['payload']['issue']
                event['payload']['issue'] = self.enrich_issue(
                    issue_data,
                    repo_name
                )
                
            return event
            
        except Exception as e:
            logger.error(f"Error enriching event {event.get('id')}: {e}")
            return None
            
    def _wait_for_rate_limit(self) -> None:
        """Implement rate limiting for API calls."""
        with self._rate_limit_lock:
            current_time = time.time()
            time_since_last_call = current_time - self._last_api_call
            
            if time_since_last_call < self.config.api.rate_limit_delay:
                time.sleep(self.config.api.rate_limit_delay - time_since_last_call)
                
            self._last_api_call = time.time()
            
    @staticmethod
    def _group_events_by_type(events: List[Dict]) -> Dict[str, List[Dict]]:
        """Group events by their type for efficient processing."""
        groups: Dict[str, List[Dict]] = {}
        for event in events:
            event_type = event.get('type')
            if event_type:
                groups.setdefault(event_type, []).append(event)
        return groups
        
    @staticmethod
    def _chunk_list(lst: List, chunk_size: int) -> List[List]:
        """Split list into chunks of specified size."""
        return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]
