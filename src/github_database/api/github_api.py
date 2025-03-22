import requests
import os
import time
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timedelta
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# Load environment variables
load_dotenv()
GITHUB_TOKEN = os.getenv('GITHUB_API_TOKEN')

# GitHub API Base URL
GITHUB_API_BASE = "https://api.github.com"

class GitHubAPIError(Exception):
    """Base exception for GitHub API errors"""
    pass

class RateLimitError(GitHubAPIError):
    """Exception for rate limit errors"""
    pass

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
    
    response = requests.get(url, headers=headers, params=params or {})
    
    if response.status_code in (403, 429):
        rate_limit_reset = int(response.headers.get('X-RateLimit-Reset', 0))
        reset_time = datetime.fromtimestamp(rate_limit_reset)
        wait_time = (reset_time - datetime.now()).total_seconds()
        raise RateLimitError(
            f"Rate limit reached. Reset in {wait_time:.0f}s at {reset_time}. "
            f"Details: {response.json()}"
        )
    elif response.status_code != 200:
        raise GitHubAPIError(f"GitHub API error: HTTP {response.status_code}")
        
    return response

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
        # Respect rate limit
        time_since_last_call = time.time() - self.last_api_call
        if time_since_last_call < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - time_since_last_call)
            
        self.last_api_call = time.time()
        
        # Make request
        url = f"{GITHUB_API_BASE}/{endpoint}"
        response = self.session.request(method, url, **kwargs)
        response.raise_for_status()
        
        return response.json()
        
    def get_repository(self, full_name: str) -> Dict:
        """Get repository information."""
        return self._make_request(f"repos/{full_name}")
        
    def get_user(self, username: str) -> Dict:
        """Get user information."""
        return self._make_request(f"users/{username}")
        
    def get_commit(self, repo_name: str, sha: str) -> Dict:
        """Get commit information."""
        return self._make_request(f"repos/{repo_name}/commits/{sha}")
        
    def get_pull_request(self, repo_name: str, number: int) -> Dict:
        """Get pull request information."""
        return self._make_request(f"repos/{repo_name}/pulls/{number}")
        
    def get_issue(self, repo_name: str, number: int) -> Dict:
        """Get issue information."""
        return self._make_request(f"repos/{repo_name}/issues/{number}")
        
    def get_contributors(self, repo_name: str) -> List[Dict]:
        """
        Get repository contributors with additional statistics.
        
        Args:
            repo_name: Full repository name (owner/repo)
            
        Returns:
            List[Dict]: List of contributor information
        """
        return self._make_request(f"repos/{repo_name}/contributors")
        
    def get_repository_statistics(self, repo_name: str) -> Dict:
        """
        Get comprehensive repository statistics.
        
        Args:
            repo_name: Full repository name (owner/repo)
            
        Returns:
            Dict: Repository statistics
        """
        stats = {
            'contributors': len(self.get_contributors(repo_name)),
            'commits': len(self._make_request(f"repos/{repo_name}/commits")),
            'issues': len(self._make_request(f"repos/{repo_name}/issues")),
            'pull_requests': len(self._make_request(f"repos/{repo_name}/pulls")),
            'releases': len(self._make_request(f"repos/{repo_name}/releases"))
        }
        
        # Get language statistics
        languages = self._make_request(f"repos/{repo_name}/languages")
        total_bytes = sum(languages.values())
        stats['languages'] = {
            lang: round(bytes_count / total_bytes * 100, 2)
            for lang, bytes_count in languages.items()
        }
        
        return stats

# Keep old search_repositories function as backup
def search_repositories(query, per_page=100, max_pages=10):
    """
    [DEPRECATED] Use get_repositories_since() instead.
    Search for repositories using the GitHub Search API.
    """
    repositories = []
    params = {
        'q': query,
        'per_page': per_page,
        'sort': 'stars',
        'order': 'desc'
    }
    
    for page in range(max_pages):
        params['page'] = page + 1
        response = make_github_request('search/repositories', params)
        data = response.json()
        
        if not data['items']:
            break
            
        repositories.extend(data['items'])
        
    return repositories