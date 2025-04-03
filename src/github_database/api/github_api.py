"""
GitHub API Client Implementation.

This module provides an optimized interface to the GitHub API with
support for:
- Token pool to bypass rate limits
- Caching for efficient requests
- Error handling and automatic retries
- Standardized data formats for repositories, contributors, and organizations
"""

import time
import logging
import requests
from datetime import datetime
from typing import Dict, List, Any, Optional
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ..config import GitHubConfig
from .token_pool import TokenPool
from .errors import GitHubAPIError, RateLimitError, AuthenticationError, NotFoundError
from .cache import MemoryCache, cached

logger = logging.getLogger(__name__)

# Constants for API endpoints
GITHUB_API_BASE = "https://api.github.com"
RATE_LIMIT_ENDPOINT = "/rate_limit"
REPOS_ENDPOINT = "/repos"
USERS_ENDPOINT = "/users"
ORGS_ENDPOINT = "/orgs"
SEARCH_REPOS_ENDPOINT = "/search/repositories"

# Default values for rate limits
DEFAULT_RATE_LIMIT = 5000
DEFAULT_RATE_LIMIT_RESET_TIME = 3600  # 1 hour


def create_repository_from_api(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert a repository object from the GitHub API into a standardized format.
    
    Args:
        data: Raw data from the GitHub API
        
    Returns:
        Standardized repository object
    """
    # Extract owner information
    owner_data = data.get('owner', {})
    owner_type = owner_data.get('type', 'User')
    
    # Set default values for missing fields
    language = data.get('language', '')
    if language is None:
        language = ''
    
    return {
        'id': data.get('id'),
        'name': data.get('name', ''),
        'full_name': data.get('full_name', ''),
        'description': data.get('description', ''),
        'url': data.get('html_url', ''),
        'api_url': data.get('url', ''),
        'created_at': data.get('created_at', ''),
        'updated_at': data.get('updated_at', ''),
        'pushed_at': data.get('pushed_at', ''),
        'homepage': data.get('homepage', ''),
        'size': data.get('size', 0),
        'stargazers_count': data.get('stargazers_count', 0),
        'watchers_count': data.get('watchers_count', 0),
        'forks_count': data.get('forks_count', 0),
        'open_issues_count': data.get('open_issues_count', 0),
        'default_branch': data.get('default_branch', 'main'),
        'is_fork': data.get('fork', False),
        'is_archived': data.get('archived', False),
        'is_disabled': data.get('disabled', False),
        'license': data.get('license', {}).get('key') if data.get('license') else None,
        'language': language,
        'topics': data.get('topics', []),
        'visibility': data.get('visibility', 'public'),
        
        # Owner information
        'owner_id': owner_data.get('id'),
        'owner_login': owner_data.get('login', ''),
        'owner_type': owner_type,
        'owner_url': owner_data.get('html_url', ''),
        'owner_avatar_url': owner_data.get('avatar_url', '')
    }


def create_user_from_api(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert a user object from the GitHub API into a standardized format.
    
    Args:
        data: Raw data from the GitHub API
        
    Returns:
        Standardized user object
    """
    return {
        'id': data.get('id'),
        'login': data.get('login', ''),
        'name': data.get('name', ''),
        'email': data.get('email', ''),
        'url': data.get('html_url', ''),
        'type': data.get('type', 'User'),
        'company': data.get('company', ''),
        'blog': data.get('blog', ''),
        'location': data.get('location', ''),
        'bio': data.get('bio', ''),
        'twitter_username': data.get('twitter_username', ''),
        'public_repos': data.get('public_repos', 0),
        'public_gists': data.get('public_gists', 0),
        'followers': data.get('followers', 0),
        'following': data.get('following', 0),
        'created_at': data.get('created_at', ''),
        'updated_at': data.get('updated_at', ''),
        'avatar_url': data.get('avatar_url', '')
    }


def create_organization_from_api(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert an organization object from the GitHub API into a standardized format.
    
    Args:
        data: Raw data from the GitHub API
        
    Returns:
        Standardized organization object
    """
    return {
        'id': data.get('id'),
        'login': data.get('login', ''),
        'name': data.get('name', ''),
        'url': data.get('html_url', ''),
        'description': data.get('description', ''),
        'company': data.get('company', ''),
        'blog': data.get('blog', ''),
        'location': data.get('location', ''),
        'email': data.get('email', ''),
        'twitter_username': data.get('twitter_username', ''),
        'is_verified': data.get('is_verified', False),
        'public_repos': data.get('public_repos', 0),
        'public_gists': data.get('public_gists', 0),
        'followers': data.get('followers', 0),
        'following': data.get('following', 0),
        'created_at': data.get('created_at', ''),
        'updated_at': data.get('updated_at', ''),
        'avatar_url': data.get('avatar_url', '')
    }


def create_contributor_from_api(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert a contributor object from the GitHub API into a standardized format.
    
    Args:
        data: Raw data from the GitHub API
        
    Returns:
        Standardized contributor object
    """
    return {
        'id': data.get('id'),
        'login': data.get('login', ''),
        'url': data.get('html_url', ''),
        'type': data.get('type', 'User'),
        'site_admin': data.get('site_admin', False),
        'contributions': data.get('contributions', 0),
        'avatar_url': data.get('avatar_url', '')
    }


class GitHubAPIClient:
    """
    Optimized GitHub API client with token pool and caching.
    
    This class manages requests to the GitHub API with the following features:
    - Token pool for efficient rate limit management
    - Memory cache for frequently queried data
    - Automatic retries and error handling
    - Standardized data formats for repositories, users, and organizations
    """
    
    def __init__(self, config: GitHubConfig, token_pool: Optional[TokenPool] = None):
        """
        Initialize GitHub API client.
        
        Args:
            config: GitHub API configuration
            token_pool: Optional TokenPool instance for multiple tokens
        """
        self.config = config
        self.token_pool = token_pool
        
        # Configure session with retries
        self.session = requests.Session()
        retries = Retry(
            total=config.retry_count,
            backoff_factor=config.retry_delay,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )
        self.session.mount('https://', HTTPAdapter(max_retries=retries))
        
        # Set default headers when not using a token pool
        if not token_pool:
            self.session.headers.update({
                'Accept': 'application/vnd.github.v3+json',
                'Authorization': f'token {config.access_token}'
            })
        else:
            # Only set Accept header, token is set per request
            self.session.headers.update({
                'Accept': 'application/vnd.github.v3+json'
            })
            
        # Initialize caches
        self.search_cache = MemoryCache(max_size=config.cache_max_size)
        
        # Status variables
        self.rate_limit = DEFAULT_RATE_LIMIT
        self.rate_limit_remaining = DEFAULT_RATE_LIMIT
        self.rate_limit_reset = time.time() + DEFAULT_RATE_LIMIT_RESET_TIME
        
        logger.info(f"GitHub API client initialized with {'token pool' if token_pool else 'single token'}")
    
    def _make_request(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Make a request to the GitHub API with proper error handling.
        
        Args:
            endpoint: API endpoint (relative to base URL)
            params: Optional query parameters
            
        Returns:
            Response data as dictionary
            
        Raises:
            RateLimitError: When rate limit is exceeded
            AuthenticationError: When authentication fails
            NotFoundError: When resource is not found
            GitHubAPIError: For other API errors
        """
        url = f"{GITHUB_API_BASE}{endpoint}"
        
        # Use token pool if available, otherwise use the configured token
        headers = {}
        if self.token_pool:
            token = self.token_pool.get_token()
            headers['Authorization'] = f'token {token}'
        
        # Add timeout to prevent hanging connections
        timeout = 30  # 30 seconds timeout for all requests
        
        try:
            logger.debug(f"Making request to {url}")
            response = self.session.get(url, params=params, headers=headers if headers else None, timeout=timeout)
            
            # Update rate limit information from headers
            if 'X-RateLimit-Limit' in response.headers:
                self.rate_limit = int(response.headers['X-RateLimit-Limit'])
            if 'X-RateLimit-Remaining' in response.headers:
                self.rate_limit_remaining = int(response.headers['X-RateLimit-Remaining'])
            if 'X-RateLimit-Reset' in response.headers:
                self.rate_limit_reset = int(response.headers['X-RateLimit-Reset'])
            
            # Handle rate limiting
            if response.status_code == 403 and self.rate_limit_remaining == 0:
                wait_time = self.rate_limit_reset - time.time()
                if wait_time > 0:
                    logger.warning(f"Rate limit exceeded. Waiting {wait_time:.1f} seconds until reset.")
                    time.sleep(min(wait_time, 60))  # Wait at most 60 seconds, then retry
                raise RateLimitError(f"Rate limit exceeded. Reset at {datetime.fromtimestamp(self.rate_limit_reset)}")
            
            # Handle other errors
            if response.status_code == 401:
                raise AuthenticationError("Authentication failed")
            if response.status_code == 404:
                raise NotFoundError(f"Resource not found: {url}")
            if response.status_code >= 400:
                raise GitHubAPIError(f"API error: {response.status_code} - {response.text}")
            
            # Return JSON data
            return response.json()
        except requests.exceptions.Timeout:
            logger.error(f"Request to {url} timed out after {timeout} seconds")
            raise GitHubAPIError(f"Request timed out after {timeout} seconds")
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Connection error for {url}: {e}")
            raise GitHubAPIError(f"Connection error: {e}")
        except (RateLimitError, AuthenticationError, NotFoundError):
            # Re-raise specific errors
            raise
        except Exception as e:
            logger.error(f"Error making request to {url}: {e}")
            raise GitHubAPIError(f"API request error: {e}")
    
    def get_rate_limit_info(self) -> Dict[str, Any]:
        """
        Get the current rate limit information.
        
        Returns:
            Dictionary with rate limit information
        """
        try:
            data = self._make_request(RATE_LIMIT_ENDPOINT)
            return data.get('resources', {})
        except Exception as e:
            logger.error(f"Error getting rate limit info: {e}")
            return {
                'core': {
                    'limit': self.rate_limit,
                    'remaining': self.rate_limit_remaining,
                    'reset': self.rate_limit_reset
                }
            }
    
    @cached(lambda self, owner, name: f"repo:{owner}/{name}")
    def get_repository(self, owner: str, name: str) -> Optional[Dict[str, Any]]:
        """
        Get repository information.
        
        Args:
            owner: Repository owner
            name: Repository name
            
        Returns:
            Standardized repository object or None if not found
        """
        try:
            endpoint = f"{REPOS_ENDPOINT}/{owner}/{name}"
            data = self._make_request(endpoint)
            return create_repository_from_api(data)
        except NotFoundError:
            logger.warning(f"Repository not found: {owner}/{name}")
            return None
        except Exception as e:
            logger.error(f"Error getting repository {owner}/{name}: {e}")
            return None
    
    @cached(lambda self, owner, name: f"repo_contributors:{owner}/{name}")
    def get_repository_contributors(self, owner: str, name: str, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get contributors for a repository.
        
        Args:
            owner: Repository owner
            name: Repository name
            limit: Maximum number of contributors to return
            
        Returns:
            List of standardized contributor objects
        """
        try:
            endpoint = f"{REPOS_ENDPOINT}/{owner}/{name}/contributors"
            params = {'per_page': min(100, limit), 'anon': 0}
            data = self._make_request(endpoint, params)
            
            contributors = [create_contributor_from_api(contributor) for contributor in data[:limit]]
            return contributors
        except Exception as e:
            logger.error(f"Error getting contributors for {owner}/{name}: {e}")
            return []
    
    @cached(lambda self, org_name: f"org:{org_name}")
    def get_organization(self, org_name: str) -> Optional[Dict[str, Any]]:
        """
        Get organization information.
        
        Args:
            org_name: Organization name
            
        Returns:
            Standardized organization object or None if not found
        """
        try:
            endpoint = f"{ORGS_ENDPOINT}/{org_name}"
            data = self._make_request(endpoint)
            return create_organization_from_api(data)
        except NotFoundError:
            logger.warning(f"Organization not found: {org_name}")
            return None
        except Exception as e:
            logger.error(f"Error getting organization {org_name}: {e}")
            return None
    
    @cached(lambda self, query: f"search:{query}")
    def search_repositories(self, query: str, sort: str = "stars", 
                           order: str = "desc", per_page: int = 100, 
                           page: int = 1, max_results: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Search for repositories.
        
        Args:
            query: Search query string
            sort: Sort field (stars, forks, updated)
            order: Sort order (desc, asc)
            per_page: Results per page
            page: Page number
            max_results: Maximum number of results to return (paginate automatically)
            
        Returns:
            List of standardized repository objects
        """
        try:
            # If max_results is specified, handle pagination automatically
            if max_results is not None:
                all_repos = []
                current_page = page
                remaining = max_results
                
                while remaining > 0:
                    # Calculate how many items to fetch in this request
                    current_per_page = min(per_page, remaining)
                    
                    params = {
                        'q': query,
                        'sort': sort,
                        'order': order,
                        'per_page': current_per_page,
                        'page': current_page
                    }
                    
                    logger.info(f"Fetching page {current_page} with {current_per_page} items per page")
                    
                    # Make the API request with timeout
                    try:
                        data = self._make_request(SEARCH_REPOS_ENDPOINT, params)
                    except GitHubAPIError as e:
                        logger.error(f"Error during repository search (page {current_page}): {e}")
                        break
                    
                    items = data.get('items', [])
                    if not items:
                        logger.info("No more repositories found")
                        break
                    
                    # Convert to standardized format
                    repositories = [create_repository_from_api(repo) for repo in items]
                    all_repos.extend(repositories)
                    
                    # Update remaining count and page
                    remaining -= len(repositories)
                    current_page += 1
                    
                    # Check if we've reached the total available results
                    total_count = data.get('total_count', 0)
                    if len(all_repos) >= total_count:
                        logger.info(f"Reached all available results ({total_count})")
                        break
                    
                    # Add a small delay between requests to avoid hitting rate limits
                    time.sleep(0.5)
                
                logger.info(f"Collected {len(all_repos)} repositories in total")
                return all_repos[:max_results]  # Ensure we don't return more than requested
            
            # Single page request
            params = {
                'q': query,
                'sort': sort,
                'order': order,
                'per_page': per_page,
                'page': page
            }
            
            data = self._make_request(SEARCH_REPOS_ENDPOINT, params)
            
            items = data.get('items', [])
            total_count = data.get('total_count', 0)
            
            logger.info(f"Found {total_count} repositories matching query: {query}")
            
            repositories = [create_repository_from_api(repo) for repo in items]
            return repositories
        except Exception as e:
            logger.error(f"Error searching repositories with query '{query}': {e}")
            return []
    
    def search_repositories_by_time(self, min_stars: int = 10, 
                                  created_after: str = "2014-01-01",
                                  language: Optional[str] = None,
                                  limit: int = 1000) -> List[Dict[str, Any]]:
        """
        Search for repositories created after a specific date with minimum stars.
        
        Args:
            min_stars: Minimum number of stars
            created_after: Creation date in YYYY-MM-DD format
            language: Optional language filter
            limit: Maximum number of repositories to return
            
        Returns:
            List of standardized repository objects
        """
        # Build query
        query_parts = [f"stars:>={min_stars}", f"created:>={created_after}"]
        
        if language:
            query_parts.append(f"language:{language}")
            
        query = " ".join(query_parts)
        
        # Determine how many pages to fetch
        pages_needed = (limit + 99) // 100  # Ceiling division by 100
        
        all_repos = []
        for page in range(1, pages_needed + 1):
            repos = self.search_repositories(query, page=page)
            all_repos.extend(repos)
            
            # Check if we got less than a full page
            if len(repos) < 100:
                break
                
            # Check if we have enough repositories
            if len(all_repos) >= limit:
                break
                
        return all_repos[:limit]
    
    def clear_search_cache(self):
        """Clear the search cache."""
        self.search_cache.clear()
        logger.info("Search cache cleared")
