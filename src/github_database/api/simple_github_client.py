"""
Simple GitHub API Client without complex dependencies.

This module provides a lightweight GitHub API client implementation
that doesn't depend on external libraries except for requests.
"""

import logging
import requests
from typing import Dict, List, Any, Optional
import time
from datetime import datetime

logger = logging.getLogger(__name__)

class SimpleGitHubClient:
    """Simple GitHub API Client without complex dependencies."""
    
    def __init__(self, token: str):
        """
        Initialize the API Client with a token.
        
        Args:
            token: GitHub API token
        """
        self.token = token
        self.session = self._create_session()
        
    def _create_session(self):
        """Create a session for HTTP requests."""
        session = requests.Session()
        session.headers.update({
            'Authorization': f'token {self.token}',
            'Accept': 'application/vnd.github.v3+json',
            'User-Agent': 'GitHub-Repository-Collector'
        })
        return session
    
    def search_repositories(self, query: str, sort: str = "stars", 
                           order: str = "desc", per_page: int = 100, 
                           max_results: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Search for repositories with the specified query.
        
        Args:
            query: Search query in GitHub syntax
            sort: Field to sort results by
            order: Sort order (asc or desc)
            per_page: Number of results per page (max 100)
            max_results: Maximum number of results to return (None for all available)
            
        Returns:
            List of repository data dictionaries
        """
        logger.info(f"Searching repositories with query: {query}")
        
        url = "https://api.github.com/search/repositories"
        params = {
            'q': query,
            'sort': sort,
            'order': order,
            'per_page': min(per_page, 100)  # GitHub API limits to 100 per page
        }
        
        all_items = []
        page = 1
        total_count = 0
        
        try:
            while True:
                # Check rate limit before making request
                rate_limit_info = self.get_rate_limit_info()
                remaining = rate_limit_info.get('resources', {}).get('search', {}).get('remaining', 0)
                reset_time = rate_limit_info.get('resources', {}).get('search', {}).get('reset', 0)
                
                if remaining <= 1:  # Keep 1 request in reserve
                    # Calculate wait time
                    current_time = time.time()
                    wait_time = max(0, reset_time - current_time) + 5  # Add 5 seconds buffer
                    
                    reset_datetime = datetime.fromtimestamp(reset_time)
                    logger.warning(f"Rate limit reached. Waiting until {reset_datetime} ({wait_time:.1f} seconds)")
                    
                    # Wait until rate limit resets
                    time.sleep(wait_time)
                
                # Add page parameter for pagination
                params['page'] = page
                
                # Make the request with retry logic
                max_retries = 3
                retry_count = 0
                while retry_count < max_retries:
                    try:
                        response = self.session.get(url, params=params)
                        response.raise_for_status()
                        break
                    except Exception as e:
                        retry_count += 1
                        if retry_count >= max_retries:
                            raise
                        logger.warning(f"Request failed, retrying ({retry_count}/{max_retries}): {e}")
                        time.sleep(2 * retry_count)  # Exponential backoff
                
                data = response.json()
                items = data.get('items', [])
                
                if page == 1:
                    total_count = data.get('total_count', 0)
                    logger.info(f"Found: {total_count} repositories")
                
                # No more items or reached the limit
                if not items:
                    break
                
                all_items.extend(items)
                
                # Calculate and display progress
                progress_percentage = min(100, (len(all_items) / total_count) * 100) if total_count > 0 else 0
                logger.info(f"Retrieved page {page}, got {len(items)} repositories, " +
                           f"total so far: {len(all_items)}/{total_count} ({progress_percentage:.1f}%)")
                
                # Check if we've reached the maximum number of results
                if max_results and len(all_items) >= max_results:
                    all_items = all_items[:max_results]
                    break
                
                # Check if we've retrieved all available items
                if len(all_items) >= total_count:
                    break
                
                # GitHub API has a limit of 1000 results (10 pages of 100)
                if page >= 10:
                    logger.warning("Reached GitHub API search limit (1000 results)")
                    break
                
                # Move to the next page
                page += 1
                
                # Add a small delay to avoid rate limiting
                time.sleep(0.5)
            
            logger.info(f"Retrieved a total of {len(all_items)} repositories")
            return all_items
            
        except Exception as e:
            logger.error(f"Error in API request: {e}")
            return all_items if all_items else []
            
    def get_repository(self, owner: str, repo: str) -> Optional[Dict[str, Any]]:
        """
        Get details for a specific repository.
        
        Args:
            owner: Repository owner/organization
            repo: Repository name
            
        Returns:
            Repository data dictionary or None if not found
        """
        url = f"https://api.github.com/repos/{owner}/{repo}"
        
        try:
            response = self.session.get(url)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error retrieving repository {owner}/{repo}: {e}")
            return None
            
    def get_repository_contributors(self, owner: str, repo: str) -> List[Dict[str, Any]]:
        """
        Get contributors for a specific repository.
        
        Args:
            owner: Repository owner/organization
            repo: Repository name
            
        Returns:
            List of contributor data dictionaries
        """
        url = f"https://api.github.com/repos/{owner}/{repo}/contributors"
        
        try:
            response = self.session.get(url)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error retrieving contributors for {owner}/{repo}: {e}")
            return []
            
    def get_organization(self, org: str) -> Optional[Dict[str, Any]]:
        """
        Get details for a specific organization.
        
        Args:
            org: Organization login/name
            
        Returns:
            Organization data dictionary or None if not found
        """
        url = f"https://api.github.com/orgs/{org}"
        
        try:
            response = self.session.get(url)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error retrieving organization {org}: {e}")
            return None
            
    def get_rate_limit_info(self) -> Dict[str, Any]:
        """
        Get information about the current rate limit status.
        
        Returns:
            Dictionary with rate limit information
        """
        try:
            response = self.session.get("https://api.github.com/rate_limit")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error getting rate limit info: {e}")
            return {"resources": {"search": {"limit": 30, "remaining": 10, "reset": int(time.time()) + 60}}}
