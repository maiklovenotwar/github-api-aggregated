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