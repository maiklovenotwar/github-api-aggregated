"""GitHub API client module."""

import requests
import os
import time
from typing import List, Dict, Optional, Tuple, Any
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from ..config.github_config import GitHubConfig

# Load environment variables
load_dotenv()
GITHUB_TOKEN = os.getenv('GITHUB_API_TOKEN')

# GitHub API Base URL
GITHUB_API_BASE = "https://api.github.com"

class GitHubAPIError(Exception):
    """Base class for GitHub API errors."""
    
    def __init__(self, message: str, status_code: Optional[int] = None):
        """Initialize error."""
        super().__init__(message)
        self.status_code = status_code
        
class RateLimitError(GitHubAPIError):
    """GitHub API rate limit error."""
    
    def __init__(self, message: str, reset_time: float):
        """Initialize error."""
        super().__init__(message, status_code=403)
        self.reset_time = reset_time
        
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type((RateLimitError, requests.exceptions.RequestException)),
    before_sleep=lambda retry_state: print(f"Retry {retry_state.attempt_number}/3 after {retry_state.idle_for:.1f}s...")
)
def make_github_request(endpoint: str, params: Optional[Dict] = None, token: Optional[str] = None) -> requests.Response:
    """
    Make a GitHub API request with retry logic.
    
    Args:
        endpoint: API endpoint (e.g. 'repositories' or 'rate_limit')
        params: Query parameters
        token: Optional GitHub API token
        
    Returns:
        Response object
        
    Raises:
        RateLimitError: On rate limit (HTTP 403/429)
        GitHubAPIError: On other API errors
    """
    # Handle full URLs vs endpoint names
    if endpoint.startswith('http'):
        url = endpoint
    else:
        url = f"{GITHUB_API_BASE}/{endpoint}"
    
    # Set up headers
    headers = {
        'Accept': 'application/vnd.github.v3+json'
    }
    if token:
        headers['Authorization'] = f'Bearer {token}'  
    elif GITHUB_TOKEN:
        headers['Authorization'] = f'Bearer {GITHUB_TOKEN}'  
    
    try:
        response = requests.get(url, headers=headers, params=params or {})
        
        # Check rate limit
        remaining = int(response.headers.get('X-RateLimit-Remaining', 0))
        if remaining == 0:
            reset_time = float(response.headers.get('X-RateLimit-Reset', 0))
            raise RateLimitError(
                f"Rate limit exceeded. Resets at {datetime.fromtimestamp(reset_time, timezone.utc)}",
                reset_time
            )
            
        # Check response
        response.raise_for_status()
        return response
        
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 403:
            # Check if rate limited
            if 'X-RateLimit-Remaining' in e.response.headers:
                reset_time = float(e.response.headers['X-RateLimit-Reset'])
                raise RateLimitError(
                    f"Rate limit exceeded. Resets at {datetime.fromtimestamp(reset_time, timezone.utc)}",
                    reset_time
                )
                
        raise GitHubAPIError(str(e), e.response.status_code)
        
    except requests.exceptions.RequestException as e:
        raise GitHubAPIError(str(e))

class GitHubAPI:
    """GitHub API client with rate limiting and caching."""
    
    def __init__(self, token: str, rate_limit_delay: float = 0.1):
        """Initialize GitHub API client."""
        self.token = token
        self.rate_limit_delay = rate_limit_delay
        self.last_api_call = 0.0
        
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {token}',
            'Accept': 'application/vnd.github.v3+json'
        })
        
    def _make_request(self, endpoint: str, method: str = 'GET', **kwargs) -> Dict:
        """
        Make API request with rate limiting and error handling.
        
        Args:
            endpoint: API endpoint (e.g. 'repositories' or 'rate_limit')
            method: HTTP method
            **kwargs: Additional request parameters
            
        Returns:
            Dict: API response
            
        Raises:
            requests.exceptions.RequestException: If request fails
        """
        # Rate limiting
        current_time = time.time()
        time_since_last_call = current_time - self.last_api_call
        if time_since_last_call < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - time_since_last_call)
        self.last_api_call = time.time()
        
        # Make request
        response = make_github_request(endpoint, token=self.token, **kwargs)
        return response.json()
        
    def get_repository(self, owner: str, name: str) -> Dict:
        """Get repository information."""
        return self._make_request(f'repos/{owner}/{name}')
        
    def get_user(self, username: str) -> Dict:
        """Get user information."""
        return self._make_request(f'users/{username}')
        
    def get_commit(self, repo_name: str, sha: str) -> Dict:
        """Get commit information."""
        owner, repo = repo_name.split('/')
        return self._make_request(f'repos/{owner}/{repo}/commits/{sha}')
        
    def get_pull_request(self, repo_name: str, number: int) -> Dict:
        """Get pull request information."""
        owner, repo = repo_name.split('/')
        return self._make_request(f'repos/{owner}/{repo}/pulls/{number}')
        
    def get_issue(self, repo_name: str, number: int) -> Dict:
        """Get issue information."""
        owner, repo = repo_name.split('/')
        return self._make_request(f'repos/{owner}/{repo}/issues/{number}')
        
    def get_contributors(self, repo_name: str) -> List[Dict]:
        """
        Get repository contributors with additional statistics.
        
        Args:
            repo_name: Full repository name (owner/repo)
            
        Returns:
            List[Dict]: List of contributor information
        """
        owner, repo = repo_name.split('/')
        return self._make_request(f'repos/{owner}/{repo}/contributors')
        
    def get_repository_statistics(self, repo_name: str) -> Dict:
        """
        Get comprehensive repository statistics.
        
        Args:
            repo_name: Full repository name (owner/repo)
            
        Returns:
            Dict: Repository statistics
        """
        owner, repo = repo_name.split('/')
        stats = {}
        
        # Get basic repository info
        repo_info = self.get_repository(owner, repo)
        stats.update({
            'stars': repo_info.get('stargazers_count', 0),
            'forks': repo_info.get('forks_count', 0),
            'watchers': repo_info.get('watchers_count', 0),
            'open_issues': repo_info.get('open_issues_count', 0),
            'language': repo_info.get('language'),
            'created_at': repo_info.get('created_at'),
            'updated_at': repo_info.get('updated_at')
        })
        
        # Get contributor stats
        try:
            contributors = self.get_contributors(repo_name)
            stats['contributors'] = len(contributors)
            stats['total_contributions'] = sum(c.get('contributions', 0) for c in contributors)
        except Exception:
            stats['contributors'] = 0
            stats['total_contributions'] = 0
            
        return stats
        
    def get_organization(self, org_name: str) -> Dict:
        """Get organization information."""
        return self._make_request(f'orgs/{org_name}')
        
    def get_organization_members(self, org_name: str) -> List[Dict]:
        """
        Get members of an organization.
        
        Args:
            org_name: Organization name
            
        Returns:
            List[Dict]: List of member information
        """
        return self._make_request(f'orgs/{org_name}/members')

def get_repositories_since(since_id: Optional[int] = None, per_page: int = 100, token: Optional[str] = None, max_repos: Optional[int] = None, since_date: Optional[datetime] = None, min_stars: int = 0) -> List[Dict]:
    """
    Get repositories from the /search/repositories endpoint, starting from the given date.
    Automatically follows the Link header for pagination.
    
    Args:
        since_id: Repository ID to start from (ignored if since_date is provided)
        per_page: Number of repositories per request (max 100)
        token: Optional GitHub API token
        max_repos: Maximum number of repositories to return
        since_date: Optional datetime to filter repositories by creation date
        min_stars: Minimum number of stars
        
    Returns:
        List of repositories
    """
    all_repositories = []
    params = {"per_page": min(per_page, 100)}  # GitHub API limit

    if since_date is not None:
        # Use search API to filter by creation date and stars
        date_str = since_date.strftime("%Y-%m-%d")
        query = f"created:>={date_str}"
        if min_stars > 0:
            query += f" stars:>={min_stars}"
        params["q"] = query
        params["sort"] = "updated"  # Sort by most recently updated
        params["order"] = "desc"
        current_url = "search/repositories"
    else:
        # Use repositories endpoint with since parameter
        if since_id is not None:
            params["since"] = since_id
        current_url = "repositories"

    while current_url:
        try:
            response = make_github_request(current_url, params, token)
            if current_url == "search/repositories":
                repositories = response.json()["items"]
            else:
                repositories = response.json()
            
            if not repositories:
                break
                
            all_repositories.extend(repositories)
            print(f"Retrieved: {len(repositories)} repositories (Total: {len(all_repositories)})")
            
            # Stop if we have enough repositories
            if max_repos and len(all_repositories) >= max_repos:
                all_repositories = all_repositories[:max_repos]
                break
            
            # Check for next page in Link header
            if 'Link' in response.headers:
                links = requests.utils.parse_header_links(response.headers['Link'])
                next_link = next((link for link in links if link['rel'] == 'next'), None)
                if next_link:
                    current_url = next_link['url']
                    params = {}  # URL already contains parameters
                else:
                    current_url = None
            else:
                current_url = None
                
        except (RateLimitError, GitHubAPIError) as e:
            print(f"Error retrieving repositories: {e}")
            break
            
    return all_repositories

class GitHubAPIClient:
    """GitHub API client class."""
    
    def __init__(self, config: GitHubConfig):
        """Initialize GitHub API client."""
        self.config = config
        
        # Configure session with retries
        self.session = requests.Session()
        retries = Retry(
            total=3,
            backoff_factor=0.3,
            status_forcelist=[500, 502, 503, 504]
        )
        self.session.mount('https://', HTTPAdapter(max_retries=retries))
        
        # Set default headers
        self.session.headers.update({
            'Accept': 'application/vnd.github.v3+json',
            'Authorization': f'token {config.access_token}'
        })
        
    def _get(self, url: str, params: Optional[Dict] = None) -> Dict:
        """Make GET request to GitHub API."""
        response = self.session.get(url, params=params)
        if response.status_code == 403 and 'X-RateLimit-Remaining' in response.headers and int(response.headers['X-RateLimit-Remaining']) == 0:
            reset_time = int(response.headers['X-RateLimit-Reset'])
            raise RateLimitError(f"Rate limit exceeded. Reset at {datetime.fromtimestamp(reset_time)}", reset_time)
        response.raise_for_status()
        return response.json()
    
    def get_repository(self, owner: str, name: str) -> Dict:
        """Get repository by owner and name."""
        url = f"{GITHUB_API_BASE}/repos/{owner}/{name}"
        return self._get(url)
    
    def get_organization(self, login: str) -> Dict:
        """Get organization by login."""
        url = f"{GITHUB_API_BASE}/orgs/{login}"
        return self._get(url)
    
    def get_user(self, login: str) -> Dict:
        """Get user by login."""
        url = f"{GITHUB_API_BASE}/users/{login}"
        return self._get(url)
        
    def search_repositories(self, min_stars: int = 10, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Search for repositories with a minimum number of stars.
        
        Args:
            min_stars: Minimum number of stars a repository must have
            limit: Maximum number of repositories to return
            
        Returns:
            List of repository data dictionaries
        """
        url = f"{GITHUB_API_BASE}/search/repositories"
        params = {
            "q": f"stars:>={min_stars}",
            "sort": "stars",
            "order": "desc",
            "per_page": min(100, limit)  # GitHub API maximum is 100 per page
        }
        
        repositories = []
        try:
            response = self._get(url, params)
            repositories = response.get("items", [])[:limit]
            
            # Format the repositories similar to BigQuery results
            formatted_repos = []
            for repo in repositories:
                formatted_repos.append({
                    "full_name": repo["full_name"],
                    "stars": repo["stargazers_count"],
                    "contributors": 0,  # This would require additional API calls to get
                    "commits": 0        # This would require additional API calls to get
                })
            
            return formatted_repos
            
        except Exception as e:
            logging.error(f"Error searching repositories: {e}")
            return []

def create_repository_from_api(data: Dict[str, Any]) -> Dict[str, Any]:
    """Create repository dictionary from API data."""
    return {
        'id': data['id'],
        'name': data['name'],
        'full_name': data['full_name'],
        'description': data.get('description'),
        'homepage': data.get('homepage'),
        'language': data.get('language'),
        'stargazers_count': data.get('stargazers_count', 0),
        'watchers_count': data.get('watchers_count', 0),
        'forks_count': data.get('forks_count', 0),
        'open_issues_count': data.get('open_issues_count', 0),
        'created_at': datetime.strptime(data['created_at'], '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc),
        'updated_at': datetime.strptime(data['updated_at'], '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc),
        'pushed_at': datetime.strptime(data['pushed_at'], '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc) if data.get('pushed_at') else None,
        'size': data.get('size', 0),
        'default_branch': data.get('default_branch', 'master'),
        'license': data.get('license', {}).get('key') if data.get('license') else None,
        'topics': data.get('topics', []),
        'has_issues': data.get('has_issues', True),
        'has_projects': data.get('has_projects', True),
        'has_wiki': data.get('has_wiki', True),
        'has_pages': data.get('has_pages', False),
        'has_downloads': data.get('has_downloads', True),
        'archived': data.get('archived', False),
        'disabled': data.get('disabled', False),
        'visibility': data.get('visibility', 'public'),
        'owner': data.get('owner'),
        'organization': data.get('organization')
    }
    
def create_user_from_api(data: Dict[str, Any]) -> Dict[str, Any]:
    """Create user dictionary from API data."""
    return {
        'id': data['id'],
        'login': data['login'],
        'name': data.get('name'),
        'email': data.get('email'),
        'bio': data.get('bio'),
        'location': data.get('location'),
        'company': data.get('company'),
        'blog': data.get('blog'),
        'twitter_username': data.get('twitter_username'),
        'public_repos': data.get('public_repos', 0),
        'public_gists': data.get('public_gists', 0),
        'followers': data.get('followers', 0),
        'following': data.get('following', 0),
        'created_at': datetime.strptime(data['created_at'], '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc),
        'updated_at': datetime.strptime(data['updated_at'], '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc),
        'type': data.get('type', 'User'),
        'site_admin': data.get('site_admin', False)
    }
    
def create_organization_from_api(data: Dict[str, Any]) -> Dict[str, Any]:
    """Create organization dictionary from API data."""
    return {
        'id': data['id'],
        'login': data['login'],
        'name': data.get('name'),
        'description': data.get('description'),
        'location': data.get('location'),
        'email': data.get('email'),
        'blog': data.get('blog'),
        'twitter_username': data.get('twitter_username'),
        'public_repos': data.get('public_repos', 0),
        'public_gists': data.get('public_gists', 0),
        'followers': data.get('followers', 0),
        'following': data.get('following', 0),
        'created_at': datetime.strptime(data['created_at'], '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc),
        'updated_at': datetime.strptime(data['updated_at'], '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc),
        'type': data.get('type', 'Organization')
    }

from ..config import GitHubConfig
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)