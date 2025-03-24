"""ETL orchestrator for GitHub data collection."""

import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional, Tuple, List
from functools import lru_cache
import time
import os
import json
import re
import threading
import queue
from concurrent.futures import ThreadPoolExecutor

from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable

from .config import ETLConfig
from .api.github_api import GitHubAPIClient, GitHubAPIError, RateLimitError
from .database.database import Contributor, Organization, Repository

logger = logging.getLogger(__name__)

class ETLOrchestrator:
    """ETL orchestrator for GitHub data collection.
    
    This simplified version focuses on collecting basic repository, organization,
    and contributor data from the GitHub API. Event data and detailed metrics are now
    handled by the aggregation module using BigQuery.
    """
    
    def __init__(self, config: ETLConfig, session_factory, token_pool=None):
        """Initialize ETL orchestrator.
        
        Args:
            config: ETL configuration
            session_factory: Session factory for database connections
            token_pool: Optional token pool for GitHub API
        """
        self.config = config
        self.session_factory = session_factory
        self.github_api = GitHubAPIClient(config.github, token_pool)
        
        # Initialisiere das Geocoding-Cache
        self.geocoding_cache_file = os.path.join(os.path.dirname(__file__), 'geocoding_cache.json')
        self.geocoding_cache = self._load_geocoding_cache()
        self.geocoding_cache_lock = threading.RLock()  # Thread-sicherer Zugriff auf den Cache
        
        # Initialisiere den Thread-Pool für Geocoding-Anfragen
        # Wir verwenden nur einen Worker, da Nominatim ohnehin nur 1 Anfrage pro Sekunde erlaubt
        self.geocoding_thread_pool = ThreadPoolExecutor(max_workers=1)
        
        # Liste von Nicht-Orten und Sonderfällen
        self.non_locations = {
            'remote', 'worldwide', 'global', 'earth', 'moon', 'mars', 'internet', 
            'web', 'online', 'virtual', 'home', 'everywhere', 'anywhere', 'nowhere',
            'n/a', 'not specified', 'not applicable', 'unknown', 'undisclosed'
        }
        
        # Länder-zu-Region-Mapping
        self.country_to_region = {
            # Nordamerika
            'US': 'North America', 'CA': 'North America', 'MX': 'North America',
            # Europa
            'GB': 'Europe', 'DE': 'Europe', 'FR': 'Europe', 'IT': 'Europe', 'ES': 'Europe',
            'NL': 'Europe', 'BE': 'Europe', 'CH': 'Europe', 'AT': 'Europe', 'SE': 'Europe',
            'NO': 'Europe', 'DK': 'Europe', 'FI': 'Europe', 'PT': 'Europe', 'IE': 'Europe',
            'PL': 'Europe', 'CZ': 'Europe', 'HU': 'Europe', 'RO': 'Europe', 'BG': 'Europe',
            'GR': 'Europe', 'HR': 'Europe', 'RS': 'Europe', 'SK': 'Europe', 'SI': 'Europe',
            'EE': 'Europe', 'LV': 'Europe', 'LT': 'Europe', 'LU': 'Europe', 'MT': 'Europe',
            'CY': 'Europe', 'IS': 'Europe', 'AL': 'Europe', 'BA': 'Europe', 'ME': 'Europe',
            'MK': 'Europe', 'MD': 'Europe', 'UA': 'Europe', 'BY': 'Europe', 'RU': 'Europe',
            # Asien
            'CN': 'Asia', 'JP': 'Asia', 'KR': 'Asia', 'IN': 'Asia', 'SG': 'Asia',
            'ID': 'Asia', 'MY': 'Asia', 'TH': 'Asia', 'VN': 'Asia', 'PH': 'Asia',
            'PK': 'Asia', 'BD': 'Asia', 'LK': 'Asia', 'NP': 'Asia', 'MM': 'Asia',
            'KH': 'Asia', 'LA': 'Asia', 'BN': 'Asia', 'MN': 'Asia', 'BT': 'Asia',
            'MV': 'Asia', 'TL': 'Asia', 'TW': 'Asia', 'HK': 'Asia',
            # Mittlerer Osten
            'IL': 'Middle East', 'TR': 'Middle East', 'SA': 'Middle East', 'AE': 'Middle East',
            'QA': 'Middle East', 'BH': 'Middle East', 'KW': 'Middle East', 'OM': 'Middle East',
            'JO': 'Middle East', 'LB': 'Middle East', 'IQ': 'Middle East', 'IR': 'Middle East',
            'SY': 'Middle East', 'PS': 'Middle East', 'YE': 'Middle East',
            # Ozeanien
            'AU': 'Oceania', 'NZ': 'Oceania', 'FJ': 'Oceania', 'PG': 'Oceania',
            'SB': 'Oceania', 'VU': 'Oceania', 'WS': 'Oceania', 'TO': 'Oceania',
            'KI': 'Oceania', 'MH': 'Oceania', 'FM': 'Oceania', 'PW': 'Oceania',
            'NR': 'Oceania', 'TV': 'Oceania',
            # Afrika
            'ZA': 'Africa', 'NG': 'Africa', 'EG': 'Africa', 'MA': 'Africa', 'KE': 'Africa',
            'GH': 'Africa', 'TZ': 'Africa', 'DZ': 'Africa', 'TN': 'Africa', 'ET': 'Africa',
            'UG': 'Africa', 'SN': 'Africa', 'CM': 'Africa', 'CI': 'Africa', 'ZM': 'Africa',
            'MZ': 'Africa', 'AO': 'Africa', 'ZW': 'Africa', 'NA': 'Africa', 'BW': 'Africa',
            'RW': 'Africa', 'MU': 'Africa', 'BJ': 'Africa', 'GA': 'Africa', 'SL': 'Africa',
            # Südamerika
            'BR': 'South America', 'AR': 'South America', 'CO': 'South America', 'CL': 'South America',
            'PE': 'South America', 'VE': 'South America', 'EC': 'South America', 'BO': 'South America',
            'PY': 'South America', 'UY': 'South America', 'GY': 'South America', 'SR': 'South America',
            'GF': 'South America',
            # Zentralamerika und Karibik
            'PA': 'Central America', 'CR': 'Central America', 'NI': 'Central America', 'HN': 'Central America',
            'SV': 'Central America', 'GT': 'Central America', 'BZ': 'Central America', 'DO': 'Central America',
            'CU': 'Central America', 'JM': 'Central America', 'HT': 'Central America', 'BS': 'Central America',
            'BB': 'Central America', 'TT': 'Central America'
        }
    
    def __del__(self):
        """Cleanup resources when the object is destroyed."""
        self.geocoding_thread_pool.shutdown(wait=False)
        
    def _load_geocoding_cache(self) -> Dict[str, Tuple[Optional[str], Optional[str]]]:
        """
        Lädt den Geocoding-Cache aus einer JSON-Datei, falls vorhanden.
        
        Returns:
            Dictionary mit Standort-Strings als Schlüssel und Tupeln (country_code, region) als Werte
        """
        if os.path.exists(self.geocoding_cache_file):
            try:
                with open(self.geocoding_cache_file, 'r', encoding='utf-8') as f:
                    cache_data = json.load(f)
                    
                    # Konvertiere Listen zurück in Tuples für interne Verwendung
                    result = {}
                    for k, v in cache_data.items():
                        if isinstance(v, list) and len(v) == 2:
                            result[k] = (v[0], v[1])
                        else:
                            result[k] = (None, None)
                    
                    logger.debug(f"Geocoding-Cache mit {len(result)} Einträgen geladen")
                    return result
            except Exception as e:
                logger.error(f"Fehler beim Laden des Geocoding-Caches: {e}")
                return {}
        logger.debug("Kein Geocoding-Cache gefunden, starte mit leerem Cache")
        return {}
        
    def _save_geocoding_cache(self):
        """Speichert den Geocoding-Cache in eine JSON-Datei."""
        try:
            # Konvertiere Tuples in Listen für JSON-Serialisierung
            cache_data = {k: list(v) if isinstance(v, tuple) else [None, None] for k, v in self.geocoding_cache.items()}
            
            with open(self.geocoding_cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, ensure_ascii=False, indent=2)
            logger.debug(f"Geocoding-Cache mit {len(self.geocoding_cache)} Einträgen gespeichert")
        except Exception as e:
            logger.error(f"Fehler beim Speichern des Geocoding-Caches: {e}")

    def _preprocess_location(self, location: str) -> Optional[str]:
        """
        Bereinigt und standardisiert Standortangaben für bessere Geocoding-Ergebnisse.
        
        Args:
            location: Roher Standort-String
            
        Returns:
            Bereinigter Standort-String oder None, wenn es sich um einen Nicht-Ort handelt
        """
        if not location or not isinstance(location, str):
            return None
            
        # Entferne Sonderzeichen und überflüssige Leerzeichen
        cleaned = re.sub(r'[^\w\s,.-]', '', location).strip()
        cleaned = re.sub(r'\s+', ' ', cleaned)
        
        # Konvertiere zu Kleinbuchstaben für den Vergleich
        lower_cleaned = cleaned.lower()
        
        # Prüfe auf Nicht-Orte und Sonderfälle
        if lower_cleaned in self.non_locations:
            return None
            
        # Prüfe auf Muster wie "Remote - USA" oder "USA (Remote)"
        remote_pattern = re.compile(r'remote|anywhere|everywhere', re.IGNORECASE)
        if remote_pattern.search(lower_cleaned):
            # Versuche, einen tatsächlichen Ort zu extrahieren
            # Entferne "remote" und ähnliche Begriffe
            parts = re.split(r'[-–—,/\(\)]', lower_cleaned)
            parts = [p.strip() for p in parts if p.strip() and not remote_pattern.search(p.strip())]
            if not parts:
                return None
            # Verwende den längsten verbleibenden Teil als Ort
            return max(parts, key=len).strip()
            
        return cleaned

    def _extract_location_data(self, location: str) -> tuple:
        """
        Extrahiert Ländercodes und Regionen aus Standort-Strings mit Geocoding.
        Diese Version prüft den Cache und gibt sofort zurück, wenn der Standort bereits bekannt ist.
        Andernfalls wird die Anfrage asynchron verarbeitet und eine Fallback-Heuristik verwendet.
        
        Args:
            location: Standort-String
            
        Returns:
            Tuple mit (country_code, region)
        """
        if not location:
            return None, None
            
        # Prüfe zuerst den Cache mit Thread-Sicherheit
        with self.geocoding_cache_lock:
            if location in self.geocoding_cache:
                return self.geocoding_cache[location]
            
        # Vorverarbeitung des Standorts
        processed_location = self._preprocess_location(location)
        if not processed_location:
            # Speichere Nicht-Orte auch im Cache
            with self.geocoding_cache_lock:
                self.geocoding_cache[location] = (None, None)
                # Wir speichern nicht bei jedem Aufruf, um die Festplattenbelastung zu reduzieren
                if len(self.geocoding_cache) % 10 == 0:
                    self._save_geocoding_cache()
            return None, None
        
        # Verwende die Heuristik als sofortigen Fallback
        country_code, region = self._extract_location_data_heuristic(location)
        
        # Speichere das vorläufige Ergebnis im Cache
        with self.geocoding_cache_lock:
            self.geocoding_cache[location] = (country_code, region)
        
        # Starte asynchrones Geocoding, nur wenn die Heuristik kein Ergebnis liefert
        if not country_code:
            # Verwende einen Future, um das Ergebnis später zu aktualisieren
            self.geocoding_thread_pool.submit(
                self._async_geocode_and_update_cache, location, processed_location
            )
        
        # Gib das vorläufige Ergebnis zurück
        return country_code, region
    
    def _async_geocode_and_update_cache(self, location: str, processed_location: str):
        """
        Führt Geocoding asynchron durch und aktualisiert den Cache.
        
        Args:
            location: Original-Standort-String
            processed_location: Vorverarbeiteter Standort-String
        """
        try:
            # Führe das Geocoding durch
            country_code, region = self._geocode_location(processed_location)
            
            # Aktualisiere den Cache nur, wenn wir ein Ergebnis haben
            with self.geocoding_cache_lock:
                self.geocoding_cache[location] = (country_code, region)
                
                # Speichere den Cache periodisch
                if len(self.geocoding_cache) % 10 == 0:
                    self._save_geocoding_cache()
                    
            logger.debug(f"Asynchrones Geocoding für '{location}' abgeschlossen: {country_code}, {region}")
        except Exception as e:
            logger.error(f"Fehler beim asynchronen Geocoding für '{location}': {e}")
            # Speichere den Fehler im Cache, damit wir nicht erneut versuchen, diesen Standort zu geocodieren
            with self.geocoding_cache_lock:
                self.geocoding_cache[location] = (None, None)
    
    def get_geocoding_stats(self) -> dict:
        """
        Gibt Statistiken über den Geocoding-Cache zurück.
        
        Returns:
            Dictionary mit Statistiken über den Geocoding-Cache
        """
        with self.geocoding_cache_lock:
            total_entries = len(self.geocoding_cache)
            successful_entries = sum(1 for v in self.geocoding_cache.values() if v[0] is not None)
            failed_entries = total_entries - successful_entries
            
            return {
                'total_entries': total_entries,
                'successful_entries': successful_entries,
                'failed_entries': failed_entries,
                'success_rate': successful_entries / total_entries if total_entries > 0 else 0
            }
        
    def _geocode_location(self, location: str) -> tuple:
        """
        Führt das eigentliche Geocoding durch.
        
        Args:
            location: Vorverarbeiteter Standort-String
            
        Returns:
            Tuple mit (country_code, region)
        """
        try:
            # Initialisiere den Geocoder mit einer benutzerdefinierten User-Agent
            # und erhöhtem Timeout (10 Sekunden statt Standard)
            geolocator = Nominatim(user_agent="github_data_collector", timeout=10)
            
            # Führe die Geocoding-Anfrage durch
            geocode_result = geolocator.geocode(location, exactly_one=True, language='en')
            
            # Warte 1 Sekunde, um die Nutzungsbeschränkungen einzuhalten
            # Nominatim erlaubt max. 1 Anfrage pro Sekunde
            time.sleep(1)
            
            if not geocode_result:
                logger.debug(f"Keine Geocoding-Ergebnisse für '{location}'")
                return None, None
                
            # Extrahiere Ländercode und Region aus dem Ergebnis
            address = geocode_result.raw.get('address', {})
            country_code = address.get('country_code', '').upper()
            
            # Bestimme die Region basierend auf dem Ländercode
            region = self._get_region_from_country_code(country_code)
            
            if country_code:
                logger.debug(f"Geocoded '{location}' to country code '{country_code}' in region '{region}'")
                return country_code, region
                
            return None, None
            
        except (GeocoderTimedOut, GeocoderUnavailable) as e:
            logger.warning(f"Geocoding error for '{location}': {e}")
            # Bei Timeout oder Nichtverfügbarkeit, versuche es nach einer Pause erneut
            try:
                # Warte 5 Sekunden vor dem erneuten Versuch
                time.sleep(5)
                geolocator = Nominatim(user_agent="github_data_collector_retry", timeout=15)
                geocode_result = geolocator.geocode(location, exactly_one=True, language='en')
                
                if not geocode_result:
                    return None, None
                    
                address = geocode_result.raw.get('address', {})
                country_code = address.get('country_code', '').upper()
                region = self._get_region_from_country_code(country_code)
                
                if country_code:
                    logger.debug(f"Geocoded '{location}' to country code '{country_code}' in region '{region}' (retry)")
                    return country_code, region
                    
                return None, None
                
            except Exception as retry_error:
                logger.warning(f"Geocoding retry failed for '{location}': {retry_error}")
                return None, None
        except Exception as e:
            logger.error(f"Unexpected geocoding error for '{location}': {e}")
            return None, None
            
    def _get_region_from_country_code(self, country_code: str) -> Optional[str]:
        """
        Bestimmt die Region basierend auf dem Ländercode.
        
        Args:
            country_code: ISO-Ländercode (2 Buchstaben)
            
        Returns:
            Region oder None, wenn der Ländercode nicht bekannt ist
        """
        if not country_code:
            return None
            
        return self.country_to_region.get(country_code.upper())
        
    def _extract_location_data_heuristic(self, location: str) -> tuple:
        """
        Fallback-Methode: Extrahiert Ländercodes und Regionen aus Standort-Strings mit einfacher Heuristik.
        
        Args:
            location: Standort-String
            
        Returns:
            Tuple mit (country_code, region)
        """
        if not location:
            return None, None
            
        # Einfache Heuristik für häufige Länder
        location_lower = location.lower()
        
        # USA
        if any(term in location_lower for term in ['usa', 'united states', 'u.s.', 'u.s.a.', 'america']):
            return 'US', 'North America'
            
        # Großbritannien
        if any(term in location_lower for term in ['uk', 'united kingdom', 'england', 'britain', 'scotland', 'wales']):
            return 'GB', 'Europe'
            
        # Kanada
        if 'canada' in location_lower:
            return 'CA', 'North America'
            
        # Deutschland
        if any(term in location_lower for term in ['germany', 'deutschland', 'berlin', 'munich', 'frankfurt']):
            return 'DE', 'Europe'
            
        # Indien
        if any(term in location_lower for term in ['india', 'bangalore', 'mumbai', 'delhi', 'hyderabad']):
            return 'IN', 'Asia'
            
        # China
        if any(term in location_lower for term in ['china', 'beijing', 'shanghai', 'shenzhen', 'guangzhou']):
            return 'CN', 'Asia'
            
        # Japan
        if any(term in location_lower for term in ['japan', 'tokyo', 'osaka', 'kyoto']):
            return 'JP', 'Asia'
            
        # Brasilien
        if any(term in location_lower for term in ['brazil', 'brasil', 'sao paulo', 'rio']):
            return 'BR', 'South America'
            
        # Australien
        if any(term in location_lower for term in ['australia', 'sydney', 'melbourne', 'brisbane']):
            return 'AU', 'Oceania'
            
        # Frankreich
        if any(term in location_lower for term in ['france', 'paris', 'lyon', 'marseille']):
            return 'FR', 'Europe'
            
        return None, None

    def _handle_api_error(self, error: Exception, context: str) -> None:
        """Handle API errors."""
        if isinstance(error, RateLimitError):
            wait_time = error.reset_time - datetime.now(timezone.utc).timestamp()
            logger.warning(f"Rate limit exceeded in {context}. Waiting {wait_time:.0f}s...")
            raise error
            
        if isinstance(error, GitHubAPIError):
            if error.status_code == 404:
                logger.warning(f"Not found in {context}: {error}")
                return
            logger.error(f"API error in {context}: {error}")
            raise error
            
        logger.error(f"Unexpected error in {context}: {error}")
        raise error
        
    def _get_or_create_contributor(self, user_data: Dict[str, Any], session: Session) -> Optional[Contributor]:
        """Get or create a contributor."""
        try:
            contributor = session.query(Contributor).filter_by(id=user_data['id']).first()
            if not contributor:
                # Prüfe, ob wir bereits alle notwendigen Daten in user_data haben
                if all(key in user_data for key in ['id', 'login', 'created_at', 'updated_at']):
                    user_details = user_data
                else:
                    # Hole detaillierte Benutzerdaten von der API
                    user_details = self.github_api.get_user(user_data['login'])
                
                # Extrahiere Land und Region aus dem Standort
                country_code, region = self._extract_location_data(user_details.get('location'))
                
                contributor = Contributor(
                    id=user_details['id'],
                    login=user_details['login'],
                    name=user_details.get('name'),
                    email=user_details.get('email'),
                    type=user_details.get('type'),
                    avatar_url=user_details.get('avatar_url'),
                    location=user_details.get('location'),
                    country_code=country_code,
                    region=region,
                    company=user_details.get('company'),
                    bio=user_details.get('bio'),
                    blog=user_details.get('blog'),
                    twitter_username=user_details.get('twitter_username'),
                    public_repos=user_details.get('public_repos', 0),
                    public_gists=user_details.get('public_gists', 0),
                    followers=user_details.get('followers', 0),
                    following=user_details.get('following', 0),
                    created_at=datetime.strptime(user_details['created_at'], '%Y-%m-%dT%H:%M:%SZ'),
                    updated_at=datetime.strptime(user_details['updated_at'], '%Y-%m-%dT%H:%M:%SZ')
                )
                session.add(contributor)
                session.commit()
            return contributor
        except Exception as e:
            logger.error(f"Error creating contributor {user_data['login']}: {e}")
            session.rollback()
            return None
            
    def _get_or_create_organization(self, org_data: Dict[str, Any], session: Session) -> Optional[Organization]:
        """Get or create an organization."""
        try:
            org = session.query(Organization).filter_by(id=org_data['id']).first()
            if not org:
                # Prüfe, ob wir bereits alle notwendigen Daten in org_data haben
                if all(key in org_data for key in ['id', 'login', 'created_at', 'updated_at']):
                    org_details = org_data
                else:
                    # Hole detaillierte Organisationsdaten von der API
                    org_details = self.github_api.get_organization(org_data['login'])
                
                # Extrahiere Land und Region aus dem Standort
                country_code, region = self._extract_location_data(org_details.get('location'))
                
                org = Organization(
                    id=org_details['id'],
                    login=org_details['login'],
                    name=org_details.get('name'),
                    bio=org_details.get('description'),  
                    blog=org_details.get('blog'),
                    location=org_details.get('location'),
                    country_code=country_code,
                    region=region,
                    email=org_details.get('email'),
                    twitter_username=org_details.get('twitter_username'),
                    public_repos=org_details.get('public_repos', 0),
                    public_gists=org_details.get('public_gists', 0),
                    followers=org_details.get('followers', 0),
                    following=org_details.get('following', 0),
                    public_members=org_details.get('public_members', 0),
                    created_at=datetime.strptime(org_details['created_at'], '%Y-%m-%dT%H:%M:%SZ'),
                    updated_at=datetime.strptime(org_details['updated_at'], '%Y-%m-%dT%H:%M:%SZ')
                )
                session.add(org)
                session.commit()
            return org
        except Exception as e:
            logger.error(f"Error creating organization {org_data['login']}: {e}")
            session.rollback()
            return None
    
    def get_existing_repository_names(self, session: Session) -> List[str]:
        """
        Ruft die Namen aller bereits in der Datenbank vorhandenen Repositories ab.
        
        Args:
            session: Datenbankverbindung
            
        Returns:
            Liste mit den vollständigen Namen (owner/name) aller vorhandenen Repositories
        """
        try:
            # Abfrage aller Repository-Namen aus der Datenbank
            existing_repos = session.query(Repository.full_name).all()
            return [repo[0] for repo in existing_repos]
        except SQLAlchemyError as e:
            logger.error(f"Fehler beim Abrufen vorhandener Repositories: {e}")
            return []

    def process_repository(self, full_name: str, session: Session) -> Optional[Repository]:
        """Process a repository by fetching its data from the GitHub API.
        
        Args:
            full_name: Full name of the repository (owner/name)
            session: Database session
            
        Returns:
            Repository object or None if an error occurred
        """
        try:
            # Prüfe, ob das Repository bereits in der Datenbank existiert
            repo = session.query(Repository).filter_by(full_name=full_name).first()
            if not repo:
                # Split the full_name into owner and name
                owner, name = full_name.split('/')
                
                # Hole Repository-Daten - diese sollten im Cache sein, wenn wir sie zuvor in search_repositories gefunden haben
                repo_data = self.github_api.get_repository(owner, name)
                owner_obj = None
                organization_id = None
                
                # Verarbeite den Besitzer des Repositories
                if repo_data['owner']['type'] == 'Organization':
                    # Prüfe, ob die Organisation bereits in der Datenbank existiert
                    owner_obj = session.query(Organization).filter_by(id=repo_data['owner']['id']).first()
                    if not owner_obj:
                        owner_obj = self._get_or_create_organization(repo_data['owner'], session)
                    if owner_obj:
                        organization_id = owner_obj.id
                else:
                    # Prüfe, ob der Benutzer bereits in der Datenbank existiert
                    owner_obj = session.query(Contributor).filter_by(id=repo_data['owner']['id']).first()
                    if not owner_obj:
                        owner_obj = self._get_or_create_contributor(repo_data['owner'], session)
                    
                if owner_obj:
                    repo = Repository(
                        id=repo_data['id'],
                        name=repo_data['name'],
                        full_name=repo_data['full_name'],
                        owner_id=owner_obj.id,
                        organization_id=organization_id,
                        description=repo_data.get('description'),
                        homepage=repo_data.get('homepage'),
                        language=repo_data.get('language'),
                        private=repo_data.get('private', False),
                        fork=repo_data.get('fork', False),
                        default_branch=repo_data.get('default_branch'),
                        size=repo_data.get('size', 0),
                        stargazers_count=repo_data.get('stargazers_count', 0),
                        watchers_count=repo_data.get('watchers_count', 0),
                        forks_count=repo_data.get('forks_count', 0),
                        open_issues_count=repo_data.get('open_issues_count', 0),
                        # Setze auch das stars- und forks-Feld für die BigQuery-Metrik
                        stars=repo_data.get('stargazers_count', 0),
                        forks=repo_data.get('forks_count', 0),
                        created_at=datetime.strptime(repo_data['created_at'], '%Y-%m-%dT%H:%M:%SZ'),
                        updated_at=datetime.strptime(repo_data['updated_at'], '%Y-%m-%dT%H:%M:%SZ'),
                        pushed_at=datetime.strptime(repo_data['pushed_at'], '%Y-%m-%dT%H:%M:%SZ') if repo_data.get('pushed_at') else None
                    )
                    session.add(repo)
                    
                    # Verarbeite die Contributors für dieses Repository
                    # Auskommentiert, da die Methode get_repository_contributors in GitHubAPIClient nicht implementiert ist
                    # self._process_repository_contributors(repo, session)
                    
                    session.commit()
            return repo
        except Exception as e:
            logger.error(f"Error processing repository {full_name}: {e}")
            session.rollback()
            return None
    
    def _process_repository_contributors(self, repo: Repository, session: Session) -> None:
        """
        Verarbeite die Contributors für ein Repository.
        
        Args:
            repo: Repository-Objekt
            session: Datenbank-Session
        """
        try:
            contributors_data = self.github_api.get_repository_contributors(
                repo.full_name.split('/')[0], 
                repo.full_name.split('/')[1]
            )
            
            for contributor_data in contributors_data:
                contributor = self._get_or_create_contributor(contributor_data, session)
                if contributor and contributor not in repo.contributors:
                    repo.contributors.append(contributor)
                    
            # Aktualisiere die Anzahl der Contributors im Repository
            repo.contributors_count = len(repo.contributors)
            
        except Exception as e:
            logger.error(f"Error processing contributors for repository {repo.full_name}: {e}")
    
    def get_quality_repositories(self, limit: int = 100, time_period: Optional[str] = None) -> list:
        """
        Retrieve repositories that meet specified quality thresholds.
        
        Args:
            limit: Maximum number of repositories to retrieve
            time_period: Optional time period for filtering repositories (format: YYYY-MM)
            
        Returns:
            List of repository data
        """
        try:
            # Get existing repository names from the database
            existing_repos = self.get_existing_repository_names(self.session_factory())
            logging.info(f"Found {len(existing_repos)} existing repositories in database")
            
            # Parse time period if provided
            created_after = None
            created_before = None
            if time_period:
                try:
                    year, month = time_period.split('-')
                    created_after = f"{year}-{month}-01"
                    
                    # Calculate the first day of the next month
                    if month == '12':
                        next_month_year = str(int(year) + 1)
                        next_month = '01'
                    else:
                        next_month_year = year
                        next_month = str(int(month) + 1).zfill(2)
                    
                    created_before = f"{next_month_year}-{next_month}-01"
                    logging.info(f"Filtering repositories created between {created_after} and {created_before}")
                except ValueError:
                    logging.warning(f"Invalid time period format: {time_period}. Expected format: YYYY-MM")
            
            # Try different sorting strategies to get diverse repositories
            sort_options = [
                {"sort_by": "stars", "sort_order": "desc"},
                {"sort_by": "forks", "sort_order": "desc"},
                {"sort_by": "updated", "sort_order": "desc"}
            ]
            
            all_repos = []
            repos_per_strategy = limit // len(sort_options)
            
            for sort_option in sort_options:
                # Search for repositories with minimum quality thresholds
                repositories = self.github_api.search_repositories(
                    min_stars=self.config.min_stars,
                    min_forks=self.config.min_forks,
                    limit=repos_per_strategy,
                    created_after=created_after,
                    created_before=created_before,
                    sort_by=sort_option["sort_by"],
                    sort_order=sort_option["sort_order"]
                )
                
                # Filter out repositories that already exist in the database
                new_repos = [repo for repo in repositories if repo["full_name"] not in existing_repos]
                all_repos.extend(new_repos)
                
                logging.info(f"Found {len(new_repos)} new quality repositories out of {len(repositories)} fetched using {sort_option['sort_by']} sorting")
                
                # If we have enough repositories, break early
                if len(all_repos) >= limit:
                    all_repos = all_repos[:limit]
                    break
            
            return all_repos
        except Exception as e:
            logger.error(f"Error getting quality repositories: {e}")
            return []
