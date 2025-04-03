"""
ETL Orchestrator for GitHub data collection.

This module coordinates data extraction via the GitHub API,
implements optimizations like caching, and orchestrates
the ETL process for GitHub repositories, contributors, and organizations.
"""

import os
import time
import logging
from typing import Dict, List, Any, Optional, Set
from pathlib import Path

from ..config import ETLConfig
from ..api import GitHubAPIClient, TokenPool
from ..api.cache import MemoryCache

logger = logging.getLogger(__name__)

class ETLOrchestrator:
    """
    Orchestrates the ETL process for GitHub data.
    
    Coordinates data collection from the GitHub API,
    implements optimizations and manages the ETL process.
    """
    
    def __init__(self, config: ETLConfig, cache_dir: Optional[str] = None):
        """
        Initialize ETL orchestrator.
        
        Args:
            config: Configuration object
            cache_dir: Optional directory for cache
        """
        self.config = config
        
        # Setup cache directory
        if cache_dir:
            self.cache_dir = Path(cache_dir)
        elif config.cache_dir:
            self.cache_dir = Path(config.cache_dir)
        else:
            self.cache_dir = Path(os.path.expanduser("~")) / ".github_database_cache"
            
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Cache directory: {self.cache_dir}")
        
        # Initialize API client
        token_pool = None
        if config.github.use_token_pool and config.github.additional_tokens:
            token_pool = TokenPool.from_config(config.github)
            
        self.github_client = GitHubAPIClient(
            config=config.github, 
            token_pool=token_pool
        )
            
        # Status and metrics
        self.metrics = {
            'repositories_processed': 0,
            'api_calls': 0,
            'cache_hits': 0,
            'start_time': time.time(),
            'errors': 0
        }
        
        logger.info(f"ETL Orchestrator initialized")
        
    def process_repository(self, full_name: str) -> Dict[str, Any]:
        """
        Process a repository and collect all related data.
        
        Args:
            full_name: Full repository name (owner/repo)
            
        Returns:
            Repository data with metadata
        """
        owner, name = full_name.split('/')
        logger.info(f"Processing repository {full_name}")
        
        try:
            # Get repository data
            repo_data = self.github_client.get_repository(owner, name)
            if not repo_data:
                logger.warning(f"Repository {full_name} not found")
                return None
                
            # Get contributors data
            contributors = self.github_client.get_repository_contributors(owner, name)
            
            # Get organization data if applicable
            org_data = None
            if repo_data.get('owner_type') == 'Organization':
                org_name = repo_data.get('owner_login')
                if org_name:
                    org_data = self.github_client.get_organization(org_name)
            
            # Update metrics
            self.metrics['repositories_processed'] += 1
            
            # Combine data
            result = {
                'repository': repo_data,
                'contributors': contributors,
                'organization': org_data
            }
            
            return result
            
        except Exception as e:
            logger.error(f"Error processing repository {full_name}: {e}")
            self.metrics['errors'] += 1
            return None
            
    def get_metrics(self) -> Dict[str, Any]:
        """
        Get current metrics for the ETL process.
        
        Returns:
            Dictionary with metrics
        """
        # Calculate runtime
        runtime = time.time() - self.metrics['start_time']
        
        # Get API stats
        api_stats = self.github_client.get_rate_limit_info()
        
        return {
            **self.metrics,
            'runtime_seconds': runtime,
            'api_rate_limit': api_stats
        }
        
    def clear_caches(self):
        """Clear all caches."""
        self.github_client.clear_search_cache()
        logger.info("All caches cleared")
