"""
GitHub Archive Data Processor

This module handles the download and processing of GitHub Archive data (https://data.gharchive.org/)
with a focus on identifying and analyzing innovation-relevant events in high-quality repositories.
"""

import gzip
import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Set, Optional, Generator
import requests
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from ..api.github_api import GitHubAPI
from ..database.database import Database

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class QualityFilters:
    """Configuration for repository quality filters."""
    min_stars: int = 50
    min_forks: int = 10
    min_commits_last_year: int = 100
    languages: Optional[Set[str]] = None
    
@dataclass
class RepositoryMetrics:
    """Store repository quality metrics."""
    id: int
    stars: int
    forks: int
    commits_last_year: int
    language: str
    last_updated: datetime

class GitHubArchiveProcessor:
    """Process GitHub Archive data with focus on innovation indicators."""
    
    BASE_URL = "https://data.gharchive.org"
    CACHE_DIR = Path("cache/github_archive")
    RELEVANT_EVENTS = {
        'PushEvent', 
        'PullRequestEvent', 
        'IssuesEvent',
        'CreateEvent',  # For new repositories/branches
        'ReleaseEvent'  # For new versions
    }
    
    def __init__(self, github_api: GitHubAPI, database: Database):
        self.github_api = github_api
        self.database = database
        self.CACHE_DIR.mkdir(parents=True, exist_ok=True)
        
    def download_daily_archive(self, date: datetime) -> List[Path]:
        """Download all hourly archive files for a specific date."""
        files = []
        for hour in range(24):
            url = f"{self.BASE_URL}/{date.strftime('%Y-%m-%d')}-{hour}.json.gz"
            cache_path = self.CACHE_DIR / f"{date.strftime('%Y-%m-%d')}-{hour}.json.gz"
            
            if not cache_path.exists():
                logger.info(f"Downloading {url}")
                response = requests.get(url, stream=True)
                response.raise_for_status()
                
                with open(cache_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
            
            files.append(cache_path)
        return files
    
    def process_archive_file(self, file_path: Path) -> Generator[Dict, None, None]:
        """Process a single archive file and yield relevant events."""
        with gzip.open(file_path, 'rt', encoding='utf-8') as f:
            for line in f:
                event = json.loads(line)
                if event['type'] in self.RELEVANT_EVENTS:
                    yield event
                    
    def filter_events_by_repo_quality(self, events: List[Dict], 
                                    quality_filters: QualityFilters) -> List[Dict]:
        """Filter events based on repository quality metrics."""
        # First, collect unique repository IDs
        repo_ids = {event['repo']['id'] for event in events}
        
        # Fetch current metrics for these repositories
        repo_metrics = self.fetch_repo_metrics(repo_ids)
        
        # Filter events based on repository metrics
        return [
            event for event in events
            if self._meets_quality_criteria(
                repo_metrics.get(event['repo']['id']), 
                quality_filters
            )
        ]
    
    def fetch_repo_metrics(self, repo_ids: Set[int]) -> Dict[int, RepositoryMetrics]:
        """Fetch current repository metrics using GitHub API."""
        metrics = {}
        
        def fetch_single_repo(repo_id: int) -> Optional[RepositoryMetrics]:
            try:
                repo_data = self.github_api.get_repository_by_id(repo_id)
                if not repo_data:
                    return None
                    
                commits_last_year = self.github_api.get_commit_count_last_year(
                    repo_data['owner']['login'],
                    repo_data['name']
                )
                
                return RepositoryMetrics(
                    id=repo_id,
                    stars=repo_data['stargazers_count'],
                    forks=repo_data['forks_count'],
                    commits_last_year=commits_last_year,
                    language=repo_data['language'] or '',
                    last_updated=datetime.now()
                )
            except Exception as e:
                logger.warning(f"Error fetching metrics for repo {repo_id}: {e}")
                return None
        
        # Use thread pool for parallel API requests
        with ThreadPoolExecutor(max_workers=5) as executor:
            results = executor.map(fetch_single_repo, repo_ids)
            
        for result in results:
            if result:
                metrics[result.id] = result
                
        return metrics
    
    def _meets_quality_criteria(self, metrics: Optional[RepositoryMetrics], 
                              filters: QualityFilters) -> bool:
        """Check if repository meets quality criteria."""
        if not metrics:
            return False
            
        return (
            metrics.stars >= filters.min_stars and
            metrics.forks >= filters.min_forks and
            metrics.commits_last_year >= filters.min_commits_last_year and
            (not filters.languages or metrics.language in filters.languages)
        )
    
    def process_github_archive_for_innovation(
        self,
        start_date: datetime,
        end_date: datetime,
        quality_filters: QualityFilters
    ) -> None:
        """
        Process GitHub Archive data for the specified date range with focus on innovation.
        
        This is a three-stage process:
        1. Collect potentially relevant repositories from archive data
        2. Filter repositories based on quality metrics
        3. Process and analyze events from qualified repositories
        """
        current_date = start_date
        while current_date <= end_date:
            logger.info(f"Processing data for {current_date.date()}")
            
            # Stage 1: Collect potential repositories
            daily_files = self.download_daily_archive(current_date)
            potential_events = []
            repo_ids = set()
            
            for file_path in daily_files:
                for event in self.process_archive_file(file_path):
                    repo_ids.add(event['repo']['id'])
                    potential_events.append(event)
            
            # Stage 2: Filter repositories by quality
            logger.info(f"Fetching metrics for {len(repo_ids)} repositories")
            repo_metrics = self.fetch_repo_metrics(repo_ids)
            
            # Stage 3: Process events for qualified repositories
            qualified_events = self.filter_events_by_repo_quality(
                potential_events, 
                quality_filters
            )
            
            # Store processed events in database
            self._store_innovation_events(qualified_events)
            
            current_date += timedelta(days=1)
    
    def _store_innovation_events(self, events: List[Dict]) -> None:
        """Store processed events in the database with innovation classification."""
        for event in events:
            # Add innovation classification logic here
            innovation_type = self._classify_innovation_event(event)
            if innovation_type:
                # Store in database with classification
                self.database.store_innovation_event(event, innovation_type)
    
    def _classify_innovation_event(self, event: Dict) -> Optional[str]:
        """
        Classify events based on their innovation relevance.
        
        Returns:
            Optional[str]: Innovation classification or None if not innovation-relevant
        """
        # Implementation of innovation classification logic
        if event['type'] == 'CreateEvent' and event.get('payload', {}).get('ref_type') == 'repository':
            return 'new_project'
            
        elif event['type'] == 'PullRequestEvent':
            # Analyze PR title and description for innovation indicators
            pr_data = event.get('payload', {}).get('pull_request', {})
            title = pr_data.get('title', '').lower()
            body = pr_data.get('body', '').lower()
            
            innovation_keywords = {
                'implement', 'introduce', 'new feature', 'enhancement',
                'improvement', 'optimization', 'redesign', 'architecture'
            }
            
            if any(keyword in title or keyword in body for keyword in innovation_keywords):
                return 'feature_innovation'
                
        elif event['type'] == 'ReleaseEvent':
            # Major version releases might indicate significant changes
            tag = event.get('payload', {}).get('release', {}).get('tag_name', '')
            if tag.startswith('v') and '.0.0' in tag:
                return 'major_release'
                
        return None
