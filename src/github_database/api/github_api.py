import requests
import os
import time
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timedelta
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# Lade die Umgebungsvariablen aus der .env-Datei
load_dotenv()
GITHUB_TOKEN = os.getenv('GITHUB_API_TOKEN')

# Setze Header für die Authentifizierung
headers = {
    'Authorization': f'token {GITHUB_TOKEN}',
    'Accept': 'application/vnd.github.v3+json'
}

class GitHubAPIError(Exception):
    """Basis-Exception für GitHub API Fehler"""
    pass

class RateLimitError(GitHubAPIError):
    """Exception für Rate Limit Fehler"""
    pass

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type((RateLimitError, requests.exceptions.RequestException)),
    before_sleep=lambda retry_state: print(f"Retry {retry_state.attempt_number}/3 nach {retry_state.idle_for:.1f}s...")
)
def make_github_request(url: str, params: Optional[Dict] = None) -> requests.Response:
    """
    Führt einen GitHub API Request mit Retry-Logik aus.
    
    Args:
        url: API URL
        params: Query Parameter
        
    Returns:
        Response Objekt
        
    Raises:
        RateLimitError: Bei Rate Limit (HTTP 403/429)
        GitHubAPIError: Bei anderen API Fehlern
    """
    response = requests.get(url, headers=headers, params=params or {})
    
    if response.status_code in (403, 429):
        rate_limit_reset = int(response.headers.get('X-RateLimit-Reset', 0))
        reset_time = datetime.fromtimestamp(rate_limit_reset)
        wait_time = (reset_time - datetime.now()).total_seconds()
        raise RateLimitError(
            f"Rate limit erreicht. Reset in {wait_time:.0f}s um {reset_time}. "
            f"Details: {response.json()}"
        )
    elif response.status_code != 200:
        raise GitHubAPIError(f"GitHub API Fehler: HTTP {response.status_code}")
        
    return response

def get_repositories_since(since_id: Optional[int] = None, per_page: int = 100) -> Tuple[List[Dict], Optional[int]]:
    """
    Ruft Repositories über den /repositories Endpunkt ab, beginnend mit der angegebenen ID.
    Folgt automatisch dem Link-Header für Paginierung.
    
    Args:
        since_id: Repository ID, ab der die Abfrage starten soll
        per_page: Anzahl der Repositories pro Anfrage (max. 100)
        
    Returns:
        Tuple aus (Liste der gefundenen Repositories, letzte Repository ID)
    """
    all_repositories = []
    current_url = "https://api.github.com/repositories"
    params = {"per_page": per_page}
    if since_id is not None:
        params["since"] = since_id

    while current_url:
        try:
            response = make_github_request(current_url, params)
            repositories = response.json()
            
            if not repositories:
                break
                
            all_repositories.extend(repositories)
            print(f"Abgerufen: {len(repositories)} Repositories (Gesamt: {len(all_repositories)})")
            
            # Folge dem Link-Header für die nächste Seite
            current_url = response.links.get('next', {}).get('url')
            # Parameter zurücksetzen, da sie in der next-URL bereits enthalten sind
            params = {}
            
            # Kleine Pause zwischen den Anfragen
            time.sleep(1)
            
        except (RateLimitError, GitHubAPIError) as e:
            print(f"API Fehler: {e}")
            break
    
    if all_repositories:
        last_id = all_repositories[-1]["id"]
        return all_repositories, last_id
    return [], None

class GitHubAPI:
    """GitHub API client with rate limiting and caching."""
    
    def __init__(self, token: str, rate_limit_delay: float = 0.1):
        """Initialize GitHub API client."""
        self.token = token
        self.rate_limit_delay = rate_limit_delay
        self.last_api_call = 0.0
        
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'token {token}',
            'Accept': 'application/vnd.github.v3+json'
        })

    def _make_request(self, url: str, method: str = 'GET', **kwargs) -> Dict:
        """
        Make API request with rate limiting and error handling.
        
        Args:
            url: API endpoint URL
            method: HTTP method
            **kwargs: Additional request parameters
            
        Returns:
            Dict: API response
            
        Raises:
            requests.exceptions.RequestException: If request fails
        """
        # Implement rate limiting
        now = time.time()
        time_since_last_call = now - self.last_api_call
        if time_since_last_call < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - time_since_last_call)
        
        try:
            response = self.session.request(method, url, **kwargs)
            self.last_api_call = time.time()
            
            # Check for rate limit
            if response.status_code == 403 and 'X-RateLimit-Remaining' in response.headers:
                reset_time = int(response.headers['X-RateLimit-Reset'])
                wait_time = reset_time - time.time()
                if wait_time > 0:
                    print(f"Rate limit hit. Waiting {wait_time:.2f} seconds")
                    time.sleep(wait_time)
                    return self._make_request(url, method, **kwargs)
                    
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            print(f"API request failed: {e}")
            raise

    def get_repository(self, full_name: str) -> Dict:
        """Get repository information."""
        url = f"https://api.github.com/repos/{full_name}"
        return self._make_request(url)

    def get_user(self, username: str) -> Dict:
        """Get user information."""
        url = f"https://api.github.com/users/{username}"
        return self._make_request(url)

    def get_commit(self, repo_name: str, sha: str) -> Dict:
        """Get commit information."""
        url = f"https://api.github.com/repos/{repo_name}/commits/{sha}"
        return self._make_request(url)

    def get_pull_request(self, repo_name: str, number: int) -> Dict:
        """Get pull request information."""
        url = f"https://api.github.com/repos/{repo_name}/pulls/{number}"
        return self._make_request(url)

    def get_issue(self, repo_name: str, number: int) -> Dict:
        """Get issue information."""
        url = f"https://api.github.com/repos/{repo_name}/issues/{number}"
        return self._make_request(url)

    def get_contributors(self, repo_name: str) -> List[Dict]:
        """
        Get repository contributors with additional statistics.
        
        Args:
            repo_name: Full repository name (owner/repo)
            
        Returns:
            List[Dict]: List of contributor information
        """
        url = f"https://api.github.com/repos/{repo_name}/contributors"
        contributors = self._make_request(url)
        
        # Enrich contributor data with additional information
        for contributor in contributors:
            try:
                # Get detailed user information
                user_data = self.get_user(contributor['login'])
                
                # Add additional fields
                contributor.update({
                    'name': user_data.get('name'),
                    'email': user_data.get('email'),
                    'company': user_data.get('company'),
                    'location': user_data.get('location'),
                    'created_at': user_data.get('created_at'),
                    'updated_at': user_data.get('updated_at')
                })
                
                # Get commit statistics
                stats_url = f"{url}/{contributor['login']}/stats"
                try:
                    stats = self._make_request(stats_url)
                    contributor['commit_stats'] = stats
                except requests.exceptions.RequestException:
                    print(f"Failed to get commit stats for {contributor['login']}")
                    
            except requests.exceptions.RequestException:
                print(f"Failed to get user data for {contributor['login']}")
                continue
                
        return contributors

    def get_repository_statistics(self, repo_name: str) -> Dict:
        """
        Get comprehensive repository statistics.
        
        Args:
            repo_name: Full repository name (owner/repo)
            
        Returns:
            Dict: Repository statistics
        """
        stats = {}
        
        try:
            # Get basic repository information
            repo_data = self.get_repository(repo_name)
            stats.update({
                'stars': repo_data['stargazers_count'],
                'forks': repo_data['forks_count'],
                'watchers': repo_data['watchers_count'],
                'open_issues': repo_data['open_issues_count'],
                'language': repo_data['language'],
                'created_at': repo_data['created_at'],
                'updated_at': repo_data['updated_at']
            })
            
            # Get contributor statistics
            stats['contributors'] = self.get_contributors(repo_name)
            
            # Get commit activity
            url = f"https://api.github.com/repos/{repo_name}/stats/commit_activity"
            try:
                commit_activity = self._make_request(url)
                stats['commit_activity'] = commit_activity
            except requests.exceptions.RequestException:
                print("Failed to get commit activity")
                
            # Get code frequency
            url = f"https://api.github.com/repos/{repo_name}/stats/code_frequency"
            try:
                code_frequency = self._make_request(url)
                stats['code_frequency'] = code_frequency
            except requests.exceptions.RequestException:
                print("Failed to get code frequency")
                
        except requests.exceptions.RequestException as e:
            print(f"Failed to get repository statistics: {e}")
            raise
            
        return stats

# Die alte search_repositories Funktion behalten wir als Backup
def search_repositories(query, per_page=100, max_pages=10):
    """
    [DEPRECATED] Nutze stattdessen get_repositories_since().
    Diese Funktion durchsucht die GitHub Search API nach Repositories anhand des übergebenen Querys.
    """
    repos = []
    for page in range(1, max_pages + 1):
        url = f"https://api.github.com/search/repositories?q={query}&per_page={per_page}&page={page}"
        print(f"Abfrage von Seite {page}...")
        try:
            response = make_github_request(url)
            data = response.json()
            items = data.get('items', [])
            if not items:
                print("Keine weiteren Ergebnisse gefunden.")
                break
            repos.extend(items)
            print(f"Seite {page}: {len(items)} Repositories gefunden.")
            time.sleep(2)
        except GitHubAPIError as e:
            print(f"Fehler: {e}")
            break
    return repos