"""
GitHub API Client Implementierung.

Dieses Modul bietet eine optimierte Schnittstelle zur GitHub API mit
Unterstützung für:
- Token-Pool zur Umgehung von Ratenbegrenzungen
- Mehrstufiges Caching für effiziente Anfragen
- Fehlerbehandlung und automatische Wiederholungsversuche
- Standardisierte Datenformate für Repositories, Benutzer und Organisationen
"""

import time
import logging
import requests
import os
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional, Tuple, Union, Set
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ..config import GitHubConfig
from .token_pool import TokenPool
from .errors import GitHubAPIError, RateLimitError, AuthenticationError, NotFoundError
from .cache import MemoryCache, DiskCache, cached

logger = logging.getLogger(__name__)

# Konstanten für API-Endpunkte
GITHUB_API_BASE = "https://api.github.com"
RATE_LIMIT_ENDPOINT = "/rate_limit"
REPOS_ENDPOINT = "/repos"
USERS_ENDPOINT = "/users"
ORGS_ENDPOINT = "/orgs"
SEARCH_REPOS_ENDPOINT = "/search/repositories"

# Standardwerte für Rate-Limit
DEFAULT_RATE_LIMIT = 5000
DEFAULT_RATE_LIMIT_RESET_TIME = 3600  # 1 Stunde


def create_repository_from_api(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Konvertiere ein Repository-Objekt aus der GitHub API in ein standardisiertes Format.
    
    Args:
        data: Rohdaten aus der GitHub API
        
    Returns:
        Standardisiertes Repository-Objekt
    """
    # Extrahiere Besitzerinformationen
    owner_data = data.get('owner', {})
    owner_type = owner_data.get('type', 'User')
    
    # Setze Standardwerte für fehlende Felder
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
        
        # Besitzerinformationen
        'owner_id': owner_data.get('id'),
        'owner_login': owner_data.get('login', ''),
        'owner_type': owner_type,
        'owner_url': owner_data.get('html_url', ''),
        'owner_avatar_url': owner_data.get('avatar_url', '')
    }


def create_user_from_api(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Konvertiere ein Benutzer-Objekt aus der GitHub API in ein standardisiertes Format.
    
    Args:
        data: Rohdaten aus der GitHub API
        
    Returns:
        Standardisiertes Benutzer-Objekt
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
    Konvertiere ein Organisations-Objekt aus der GitHub API in ein standardisiertes Format.
    
    Args:
        data: Rohdaten aus der GitHub API
        
    Returns:
        Standardisiertes Organisations-Objekt
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


class GitHubAPIClient:
    """
    Optimierter GitHub API-Client mit Token-Pool und mehrschichtigem Caching.
    
    Diese Klasse verwaltet Anfragen an die GitHub API mit folgenden Features:
    - Token-Pool für effiziente Ratenbegrenzungsverwaltung
    - In-Memory-Cache für häufig abgefragte Daten
    - Disk-Cache für persistente Daten
    - Automatische Wiederholungsversuche und Fehlerbehandlung
    - Standardisierte Datenformate für Repositories, Benutzer und Organisationen
    """
    
    def __init__(self, config: GitHubConfig, token_pool: Optional[TokenPool] = None, 
                 cache_dir: Optional[str] = None):
        """
        Initialisiere GitHub API-Client.
        
        Args:
            config: GitHub API-Konfiguration
            token_pool: Optional TokenPool-Instanz für multiple Tokens
            cache_dir: Optional Verzeichnis für Disk-Cache
        """
        self.config = config
        self.token_pool = token_pool
        
        # Konfiguriere Session mit Wiederholungsversuchen
        self.session = requests.Session()
        retries = Retry(
            total=config.retry_count,
            backoff_factor=config.retry_delay,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )
        self.session.mount('https://', HTTPAdapter(max_retries=retries))
        
        # Setze Standard-Headers wenn kein Token-Pool verwendet wird
        if not token_pool:
            self.session.headers.update({
                'Accept': 'application/vnd.github.v3+json',
                'Authorization': f'token {config.access_token}'
            })
        else:
            # Nur Accept-Header setzen, Token wird pro Anfrage gesetzt
            self.session.headers.update({
                'Accept': 'application/vnd.github.v3+json'
            })
        
        # Initialisiere Caches
        cache_size = 10000  # Standard-Cachegröße
        if cache_dir:
            self.disk_cache = DiskCache("github_api", cache_dir)
        else:
            self.disk_cache = None
            
        self.repo_cache = MemoryCache("repository", cache_size)
        self.user_cache = MemoryCache("user", 5000)
        self.org_cache = MemoryCache("organization", 1000)
        self.search_cache = MemoryCache("search", 500)
        
        logger.info(f"GitHub API-Client initialisiert mit {'Token-Pool' if token_pool else 'einzelnem Token'}")
        
    def _get_api_url(self, endpoint: str) -> str:
        """
        Erzeuge vollständige API-URL.
        
        Args:
            endpoint: API-Endpunkt
            
        Returns:
            Vollständige API-URL
        """
        # Entferne führenden Schrägstrich, falls vorhanden
        if endpoint.startswith('/'):
            endpoint = endpoint[1:]
            
        return f"{self.config.api_url}/{endpoint}"
        
    def _get(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Führe GET-Anfrage an die GitHub API aus.
        
        Diese Methode verwaltet Token-Verwendung, Rate-Limits und Fehlerbehandlung.
        
        Args:
            endpoint: API-Endpunkt
            params: Query-Parameter
            
        Returns:
            API-Antwort als Dictionary
            
        Raises:
            GitHubAPIError: Bei API-Fehlern
            RateLimitError: Bei Überschreitung des Rate-Limits
            AuthenticationError: Bei Authentifizierungsproblemen
        """
        url = self._get_api_url(endpoint)
        token_idx = 0
        token = self.config.access_token
        
        # Hole Token aus Pool, falls verfügbar
        if self.token_pool:
            token, token_idx = self.token_pool.get_token()
            self.session.headers.update({'Authorization': f'token {token}'})
        
        try:
            # Kleine Verzögerung, um API-Limits zu respektieren
            time.sleep(self.config.rate_limit_delay)
            
            # Anfrage ausführen
            logger.debug(f"GET {url} (Token: {token_idx if self.token_pool else 'default'})")
            response = self.session.get(url, params=params)
            
            # Rate-Limit-Informationen extrahieren
            remaining = int(response.headers.get('X-RateLimit-Remaining', DEFAULT_RATE_LIMIT))
            reset_time = float(response.headers.get('X-RateLimit-Reset', time.time() + DEFAULT_RATE_LIMIT_RESET_TIME))
            
            # Rate-Limit im Token-Pool aktualisieren
            if self.token_pool:
                self.token_pool.update_rate_limit(token_idx, remaining, reset_time)
            
            # HTTP-Fehler behandeln
            if response.status_code >= 400:
                error_data = response.json() if response.text else {}
                
                # Spezifische Fehlertypen
                if response.status_code == 403 and 'rate limit exceeded' in response.text.lower():
                    raise RateLimitError(
                        f"Rate-Limit überschritten: {error_data.get('message', '')}",
                        reset_time,
                        403,
                        error_data
                    )
                elif response.status_code == 401:
                    if self.token_pool:
                        self.token_pool.register_error(token_idx, 'auth')
                    raise AuthenticationError(
                        f"Authentifizierungsfehler: {error_data.get('message', '')}",
                        401,
                        token_idx if self.token_pool else None,
                        error_data
                    )
                elif response.status_code == 404:
                    resource_path = endpoint.split('/')
                    resource_type = resource_path[0] if resource_path else 'unknown'
                    resource_id = '/'.join(resource_path[1:]) if len(resource_path) > 1 else 'unknown'
                    raise NotFoundError(
                        f"Ressource nicht gefunden: {endpoint}",
                        resource_type,
                        resource_id,
                        404
                    )
                else:
                    raise GitHubAPIError(
                        f"GitHub API-Fehler ({response.status_code}): {error_data.get('message', '')}",
                        response.status_code,
                        error_data
                    )
            
            return response.json()
            
        except requests.exceptions.RequestException as e:
            if self.token_pool:
                self.token_pool.register_error(token_idx, 'network')
            raise GitHubAPIError(f"Netzwerkfehler bei Anfrage an {url}: {str(e)}")
            
    def get_rate_limit(self) -> Dict[str, Any]:
        """
        Hole Rate-Limit-Informationen von der GitHub API.
        
        Returns:
            Informationen über aktuelle Rate-Limits
        """
        return self._get(RATE_LIMIT_ENDPOINT)
        
    def get_repository(self, owner: str, name: str) -> Dict[str, Any]:
        """
        Hole Repository-Informationen.
        
        Versucht zuerst, die Daten aus dem Cache zu holen, und führt dann
        bei Bedarf eine API-Anfrage durch.
        
        Args:
            owner: Repository-Besitzer (Benutzer oder Organisation)
            name: Repository-Name
            
        Returns:
            Standardisierte Repository-Informationen
        """
        cache_key = f"{owner}/{name}"
        cached_repo = self.repo_cache.get(cache_key)
        
        if cached_repo:
            return cached_repo
            
        # Zusätzliche Disk-Cache-Prüfung
        if self.disk_cache:
            disk_cached_repo = self.disk_cache.get(f"repository:{cache_key}")
            if disk_cached_repo:
                self.repo_cache.set(cache_key, disk_cached_repo)
                return disk_cached_repo
        
        # Anfrage an die API
        try:
            repo_data = self._get(f"{REPOS_ENDPOINT}/{owner}/{name}")
            repo = create_repository_from_api(repo_data)
            
            # In Caches speichern
            self.repo_cache.set(cache_key, repo)
            if self.disk_cache:
                self.disk_cache.set(f"repository:{cache_key}", repo)
                
            return repo
            
        except NotFoundError:
            logger.warning(f"Repository {owner}/{name} nicht gefunden")
            return None
        except GitHubAPIError as e:
            logger.error(f"Fehler beim Abrufen des Repositories {owner}/{name}: {e}")
            return None
    
    def get_user(self, login: str) -> Dict[str, Any]:
        """
        Hole Benutzerinformationen.
        
        Args:
            login: GitHub-Benutzername
            
        Returns:
            Standardisierte Benutzerinformationen oder None bei Fehler
        """
        cached_user = self.user_cache.get(login)
        if cached_user:
            return cached_user
            
        try:
            user_data = self._get(f"{USERS_ENDPOINT}/{login}")
            user = create_user_from_api(user_data)
            self.user_cache.set(login, user)
            return user
        except (NotFoundError, GitHubAPIError) as e:
            logger.warning(f"Fehler beim Abrufen des Benutzers {login}: {e}")
            return None
    
    def get_organization(self, login: str) -> Dict[str, Any]:
        """
        Hole Organisationsinformationen.
        
        Args:
            login: GitHub-Organisationsname
            
        Returns:
            Standardisierte Organisationsinformationen oder None bei Fehler
        """
        cached_org = self.org_cache.get(login)
        if cached_org:
            return cached_org
            
        try:
            org_data = self._get(f"{ORGS_ENDPOINT}/{login}")
            org = create_organization_from_api(org_data)
            self.org_cache.set(login, org)
            return org
        except (NotFoundError, GitHubAPIError) as e:
            logger.warning(f"Fehler beim Abrufen der Organisation {login}: {e}")
            return None
    
    def search_repositories(self, min_stars: int = 10, max_stars: Optional[int] = None, 
                          min_forks: int = 0, limit: int = 100, language: Optional[str] = None,
                          created_after: Optional[str] = None, created_before: Optional[str] = None,
                          sort_by: str = "stars", sort_order: str = "desc") -> List[Dict[str, Any]]:
        """
        Suche nach Repositories mit Qualitätsfiltern.
        
        Args:
            min_stars: Mindestanzahl von Sternen
            max_stars: Maximale Anzahl von Sternen (optional)
            min_forks: Mindestanzahl von Forks
            limit: Maximale Anzahl zurückzugebender Repositories
            language: Filter für Programmiersprache
            created_after: Filter für nach Datum erstellte Repositories (YYYY-MM-DD)
            created_before: Filter für vor Datum erstellte Repositories (YYYY-MM-DD)
            sort_by: Sortierfeld ("stars", "forks", "updated", "help-wanted-issues")
            sort_order: Sortierreihenfolge ("desc" oder "asc")
            
        Returns:
            Liste von standardisierten Repository-Objekten
        """
        # Suchquery erstellen
        query_parts = [f"stars:>={min_stars}"]
        
        if max_stars:
            query_parts.append(f"stars:<={max_stars}")
            
        if min_forks > 0:
            query_parts.append(f"forks:>={min_forks}")
            
        if language:
            query_parts.append(f"language:{language}")
            
        if created_after:
            query_parts.append(f"created:>={created_after}")
            
        if created_before:
            query_parts.append(f"created:<={created_before}")
            
        query = " ".join(query_parts)
        
        # Cacheabfrage
        cache_key = f"search:{query}:{sort_by}:{sort_order}:{limit}"
        cached_results = self.search_cache.get(cache_key)
        if cached_results:
            return cached_results
        
        # Parameter für die API-Anfrage
        params = {
            "q": query,
            "sort": sort_by,
            "order": sort_order,
            "per_page": min(100, limit)  # GitHub API erlaubt max. 100 pro Seite
        }
        
        try:
            collected_repos = []
            page = 1
            
            # Paginierung implementieren
            while len(collected_repos) < limit:
                params["page"] = page
                search_data = self._get(SEARCH_REPOS_ENDPOINT, params)
                
                items = search_data.get("items", [])
                if not items:
                    break
                    
                # Repositories konvertieren und zum Ergebnis hinzufügen
                for item in items:
                    repo = create_repository_from_api(item)
                    
                    # Füge das Repository auch zum Repository-Cache hinzu
                    self.repo_cache.set(repo["full_name"], repo)
                    
                    collected_repos.append(repo)
                    if len(collected_repos) >= limit:
                        break
                
                # Prüfen, ob es weitere Seiten gibt
                if len(items) < params["per_page"]:
                    break
                    
                page += 1
            
            # Ergebnisse cachen
            self.search_cache.set(cache_key, collected_repos)
            
            return collected_repos
            
        except GitHubAPIError as e:
            logger.error(f"Fehler bei der Repository-Suche: {e}")
            return []
    
    def get_repository_contributors(self, repo_full_name: str, max_contributors: int = 100) -> List[Dict[str, Any]]:
        """
        Hole Mitwirkende eines Repositories.
        
        Args:
            repo_full_name: Vollständiger Repository-Name (owner/repo)
            max_contributors: Maximale Anzahl zurückzugebender Mitwirkender
            
        Returns:
            Liste von Mitwirkenden mit Beitragsinformationen
        """
        try:
            # API-Anfrage für Mitwirkende
            endpoint = f"{REPOS_ENDPOINT}/{repo_full_name}/contributors"
            params = {"per_page": min(100, max_contributors), "anon": "false"}
            
            contributors_data = self._get(endpoint, params)
            
            # Daten formatieren
            contributors = []
            for contributor in contributors_data:
                contributor_info = {
                    'id': contributor.get('id'),
                    'login': contributor.get('login'),
                    'type': contributor.get('type'),
                    'contributions': contributor.get('contributions', 0),
                    'url': contributor.get('html_url')
                }
                contributors.append(contributor_info)
                
                # Auch gleich Benutzerdetails cachen, wenn verfügbar
                if contributor.get('login'):
                    user_data = {
                        'id': contributor.get('id'),
                        'login': contributor.get('login'),
                        'type': contributor.get('type'),
                        'url': contributor.get('html_url'),
                        'avatar_url': contributor.get('avatar_url')
                    }
                    self.user_cache.set(contributor['login'], user_data)
            
            return contributors
            
        except GitHubAPIError as e:
            logger.warning(f"Fehler beim Abrufen der Mitwirkenden für {repo_full_name}: {e}")
            return []
    
    def get_repository_languages(self, repo_full_name: str) -> Dict[str, int]:
        """
        Hole Sprachstatistiken eines Repositories.
        
        Args:
            repo_full_name: Vollständiger Repository-Name (owner/repo)
            
        Returns:
            Dictionary mit Sprachen als Schlüssel und Bytes als Werte
        """
        try:
            endpoint = f"{REPOS_ENDPOINT}/{repo_full_name}/languages"
            return self._get(endpoint)
        except GitHubAPIError as e:
            logger.warning(f"Fehler beim Abrufen der Sprachen für {repo_full_name}: {e}")
            return {}
    
    def get_repository_topics(self, repo_full_name: str) -> List[str]:
        """
        Hole Themen eines Repositories.
        
        Args:
            repo_full_name: Vollständiger Repository-Name (owner/repo)
            
        Returns:
            Liste von Themen-Tags
        """
        try:
            # Die GitHub API benötigt einen speziellen Accept-Header für Themen
            old_accept = self.session.headers.get('Accept')
            self.session.headers.update({'Accept': 'application/vnd.github.mercy-preview+json'})
            
            endpoint = f"{REPOS_ENDPOINT}/{repo_full_name}/topics"
            data = self._get(endpoint)
            
            # Header wiederherstellen
            self.session.headers.update({'Accept': old_accept})
            
            return data.get('names', [])
        except GitHubAPIError as e:
            logger.warning(f"Fehler beim Abrufen der Themen für {repo_full_name}: {e}")
            return []
    
    def get_repository_with_details(self, owner: str, name: str) -> Dict[str, Any]:
        """
        Hole umfassende Repository-Informationen mit zusätzlichen Details.
        
        Diese Methode kombiniert mehrere API-Anfragen, um ein vollständiges
        Bild eines Repositories zu erhalten.
        
        Args:
            owner: Repository-Besitzer
            name: Repository-Name
            
        Returns:
            Erweitertes Repository-Objekt mit zusätzlichen Details
        """
        repo_full_name = f"{owner}/{name}"
        
        # Basis-Repository-Informationen abrufen
        repo = self.get_repository(owner, name)
        if not repo:
            return None
            
        # Zusätzliche Informationen hinzufügen
        try:
            # Sprachen
            repo['languages'] = self.get_repository_languages(repo_full_name)
            
            # Themen (falls nicht bereits in der Basis-Anfrage enthalten)
            if not repo.get('topics'):
                repo['topics'] = self.get_repository_topics(repo_full_name)
                
            # Top-Mitwirkende (begrenzt auf 5 für Effizienz)
            top_contributors = self.get_repository_contributors(repo_full_name, 5)
            repo['top_contributors'] = top_contributors
            
            # Besitzerdetails basierend auf Typ abrufen
            if repo['owner_type'] == 'Organization':
                owner_details = self.get_organization(repo['owner_login'])
            else:
                owner_details = self.get_user(repo['owner_login'])
                
            if owner_details:
                repo['owner_details'] = owner_details
            
            return repo
            
        except GitHubAPIError as e:
            logger.warning(f"Fehler beim Abrufen erweiterter Details für {repo_full_name}: {e}")
            return repo  # Rückgabe der Basis-Informationen
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """
        Hole Cache-Statistiken.
        
        Returns:
            Dictionary mit Cache-Statistiken
        """
        stats = {
            'repository_cache': self.repo_cache.stats(),
            'user_cache': self.user_cache.stats(),
            'organization_cache': self.org_cache.stats(),
            'search_cache': self.search_cache.stats()
        }
        
        if self.disk_cache:
            stats['disk_cache'] = self.disk_cache.stats()
            
        return stats
    
    def clear_caches(self) -> None:
        """Leere alle Caches."""
        self.repo_cache.clear()
        self.user_cache.clear()
        self.org_cache.clear()
        self.search_cache.clear()
        
        if self.disk_cache:
            self.disk_cache.clear()
            
        logger.info("Alle API-Caches wurden geleert")
    
    def clear_search_cache(self) -> None:
        """Leere nur den Such-Cache für frische Suchergebnisse."""
        self.search_cache.clear()
        logger.info("Such-Cache wurde geleert")
    
    def get_api_statistics(self) -> Dict[str, Any]:
        """
        Gibt Statistiken zur API-Nutzung zurück.
        
        Returns:
            Dictionary mit API-Nutzungsstatistiken
        """
        stats = {
            'requests': getattr(self, '_request_count', 0),
            'cache_hits': sum([
                self.repo_cache.stats().get('hits', 0),
                self.user_cache.stats().get('hits', 0),
                self.org_cache.stats().get('hits', 0),
                self.search_cache.stats().get('hits', 0)
            ])
        }
        
        if hasattr(self, '_rate_limit_remaining'):
            stats['rate_limit_remaining'] = self._rate_limit_remaining
            
        return stats
    
    @classmethod
    def from_config(cls, config, cache_dir: Optional[str] = None):
        """
        Erstelle GitHubAPIClient aus Konfiguration.
        
        Diese Factory-Methode erstellt einen Client mit optionalem Token-Pool
        basierend auf der Konfiguration.
        
        Args:
            config: Konfigurationsobjekt mit GitHub-Einstellungen
            cache_dir: Optionales Cache-Verzeichnis
            
        Returns:
            Konfigurierter GitHubAPIClient
        """
        token_pool = None
        if config.github.use_token_pool and config.github.additional_tokens:
            token_pool = TokenPool.from_config(config.github)
            
        return cls(config.github, token_pool, cache_dir=cache_dir if cache_dir else config.cache_dir)
