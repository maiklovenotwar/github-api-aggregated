#!/usr/bin/env python3
"""
Updates country codes for contributors and organizations in the database
by geocoding existing location information.
"""

import os
import sys
import time
import json
import logging
import argparse
from pathlib import Path
from typing import Dict, Tuple, Optional, List
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
import re

# Add project directory to Python path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Import project modules
from github_database.database.database import Base, Contributor, Organization
from github_database.config.config import load_config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class GeocodingUpdater:
    """Class for updating geocoding information in the database."""
    
    def __init__(self, db_url: str, cache_file: str = "geocoding_cache.json", ignore_cache: bool = False):
        """
        Initialize the geocoding updater.
        
        Args:
            db_url: Database URL
            cache_file: Path to cache file
            ignore_cache: If True, ignore cache and perform new geocoding
        """
        self.db_url = db_url
        self.cache_file = cache_file
        self.ignore_cache = ignore_cache
        self.engine = create_engine(db_url)
        self.Session = sessionmaker(bind=self.engine)
        
        # Initialize geocoding cache
        self.geocoding_cache = self._load_geocoding_cache()
        
        # List of non-locations and special cases
        self.non_locations = {
            'remote', 'worldwide', 'global', 'earth', 'moon', 'mars', 'internet', 
            'web', 'online', 'virtual', 'home', 'everywhere', 'anywhere', 'nowhere',
            'n/a', 'not specified', 'not applicable', 'unknown', 'undisclosed'
        }
        
        # Country-to-region mapping
        self.country_to_region = {
            # North America
            'US': 'North America', 'CA': 'North America', 'MX': 'North America',
            # Europe
            'GB': 'Europe', 'DE': 'Europe', 'FR': 'Europe', 'IT': 'Europe', 'ES': 'Europe',
            'NL': 'Europe', 'BE': 'Europe', 'CH': 'Europe', 'AT': 'Europe', 'SE': 'Europe',
            'NO': 'Europe', 'DK': 'Europe', 'FI': 'Europe', 'PT': 'Europe', 'IE': 'Europe',
            'PL': 'Europe', 'CZ': 'Europe', 'HU': 'Europe', 'RO': 'Europe', 'BG': 'Europe',
            'GR': 'Europe', 'HR': 'Europe', 'RS': 'Europe', 'SK': 'Europe', 'SI': 'Europe',
            'EE': 'Europe', 'LV': 'Europe', 'LT': 'Europe', 'LU': 'Europe', 'MT': 'Europe',
            'CY': 'Europe', 'IS': 'Europe', 'AL': 'Europe', 'BA': 'Europe', 'ME': 'Europe',
            'MK': 'Europe', 'MD': 'Europe', 'UA': 'Europe', 'BY': 'Europe', 'RU': 'Europe',
            # Asia
            'CN': 'Asia', 'JP': 'Asia', 'KR': 'Asia', 'IN': 'Asia', 'SG': 'Asia',
            'ID': 'Asia', 'MY': 'Asia', 'TH': 'Asia', 'VN': 'Asia', 'PH': 'Asia',
            'PK': 'Asia', 'BD': 'Asia', 'LK': 'Asia', 'NP': 'Asia', 'MM': 'Asia',
            'KH': 'Asia', 'LA': 'Asia', 'BN': 'Asia', 'MN': 'Asia', 'BT': 'Asia',
            'MV': 'Asia', 'TL': 'Asia', 'TW': 'Asia', 'HK': 'Asia',
            # Middle East
            'IL': 'Middle East', 'TR': 'Middle East', 'SA': 'Middle East', 'AE': 'Middle East',
            'QA': 'Middle East', 'BH': 'Middle East', 'KW': 'Middle East', 'OM': 'Middle East',
            'JO': 'Middle East', 'LB': 'Middle East', 'IQ': 'Middle East', 'IR': 'Middle East',
            'SY': 'Middle East', 'PS': 'Middle East', 'YE': 'Middle East',
            # Oceania
            'AU': 'Oceania', 'NZ': 'Oceania', 'FJ': 'Oceania', 'PG': 'Oceania',
            'SB': 'Oceania', 'VU': 'Oceania', 'WS': 'Oceania', 'TO': 'Oceania',
            'KI': 'Oceania', 'MH': 'Oceania', 'FM': 'Oceania', 'PW': 'Oceania',
            'NR': 'Oceania', 'TV': 'Oceania',
            # Africa
            'ZA': 'Africa', 'NG': 'Africa', 'EG': 'Africa', 'MA': 'Africa', 'KE': 'Africa',
            'GH': 'Africa', 'TZ': 'Africa', 'DZ': 'Africa', 'TN': 'Africa', 'ET': 'Africa',
            'UG': 'Africa', 'SN': 'Africa', 'CM': 'Africa', 'CI': 'Africa', 'ZM': 'Africa',
            'MZ': 'Africa', 'AO': 'Africa', 'ZW': 'Africa', 'NA': 'Africa', 'BW': 'Africa',
            'RW': 'Africa', 'MU': 'Africa', 'BJ': 'Africa', 'GA': 'Africa', 'SL': 'Africa',
            # South America
            'BR': 'South America', 'AR': 'South America', 'CO': 'South America', 'CL': 'South America',
            'PE': 'South America', 'VE': 'South America', 'EC': 'South America', 'BO': 'South America',
            'PY': 'South America', 'UY': 'South America', 'GY': 'South America', 'SR': 'South America',
            'GF': 'South America',
            # Central America and Caribbean
            'PA': 'Central America', 'CR': 'Central America', 'NI': 'Central America', 'HN': 'Central America',
            'SV': 'Central America', 'GT': 'Central America', 'BZ': 'Central America', 'DO': 'Central America',
            'CU': 'Central America', 'JM': 'Central America', 'HT': 'Central America', 'BS': 'Central America',
            'BB': 'Central America', 'TT': 'Central America'
        }
        
    def _load_geocoding_cache(self):
        """
        Load geocoding cache from JSON file if it exists.
        
        Returns:
            Dictionary with location strings as keys and tuples (country_code, region) as values
        """
        if self.ignore_cache:
            logger.info("Ignoring geocoding cache")
            return {}
            
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    cache_data = json.load(f)
                    
                # Convert from dict to tuple format
                cache = {}
                for location, data in cache_data.items():
                    if isinstance(data, list) and len(data) == 2:
                        cache[location] = tuple(data)
                    elif data is not None:
                        cache[location] = (data.get('country_code'), data.get('region'))
                    else:
                        # Handle None data case
                        cache[location] = (None, None)
                        
                logger.info(f"Loaded {len(cache)} location entries from cache")
                return cache
            except Exception as e:
                logger.error(f"Error loading geocoding cache: {e}")
                return {}
        else:
            logger.info("No geocoding cache found, starting fresh")
            return {}
            
    def _save_geocoding_cache(self):
        """Save geocoding cache to JSON file."""
        try:
            # Convert to serializable format
            cache_data = {}
            for location, (country_code, region) in self.geocoding_cache.items():
                cache_data[location] = [country_code, region]
                
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, ensure_ascii=False, indent=2)
                
            logger.info(f"Saved {len(self.geocoding_cache)} location entries to cache")
        except Exception as e:
            logger.error(f"Error saving geocoding cache: {e}")
            
    def _preprocess_location(self, location: str) -> Optional[str]:
        """
        Clean and standardize location strings for better geocoding results.
        
        Args:
            location: Raw location string
            
        Returns:
            Cleaned location string or None if it's a non-location
        """
        if not location:
            return None
            
        # Convert to lowercase for consistency
        location = location.lower()
        
        # Skip non-locations
        if location in self.non_locations:
            return None
            
        # Skip nonsensical short strings
        if len(location) < 3 and not location.isalpha():
            return None
            
        # Remove URL schemes and domains
        if '://' in location or location.startswith('www.'):
            return None
            
        # Clean up special characters and normalize
        location = re.sub(r'[^\w\s,.-]', '', location)
        location = re.sub(r'\s+', ' ', location).strip()
        
        # Skip strings like "n/a", "null", etc.
        if location in ['na', 'n/a', 'null', 'none', 'nil', 'undefined', '-']:
            return None
            
        # Skip if only emojis or symbols remain
        if not any(c.isalpha() for c in location):
            return None
            
        return location
        
    def _extract_country_from_text(self, location: str) -> Optional[str]:
        """
        Try to extract country code directly from location text.
        
        Args:
            location: Location string
            
        Returns:
            ISO country code or None if not found
        """
        # Check for country codes in brackets like "City (US)" or "City, US"
        match = re.search(r'\(([A-Z]{2})\)|\s([A-Z]{2})$|,\s*([A-Z]{2})$', location.upper())
        if match:
            country_code = next((g for g in match.groups() if g is not None), None)
            if country_code in self.country_to_region:
                return country_code
                
        # Check for common country names
        location_lower = location.lower()
        country_mapping = {
            # North America
            'united states': 'US', 'usa': 'US', 'u.s.a': 'US', 'u.s.': 'US', 'america': 'US',
            'united states of america': 'US', 'the united states': 'US', 'the us': 'US',
            'canada': 'CA', 'mexico': 'MX',
            
            # Europe
            'united kingdom': 'GB', 'uk': 'GB', 'britain': 'GB', 'great britain': 'GB', 
            'england': 'GB', 'scotland': 'GB', 'wales': 'GB', 'northern ireland': 'GB',
            'germany': 'DE', 'deutschland': 'DE', 'france': 'FR', 'italy': 'IT', 'italia': 'IT',
            'spain': 'ES', 'españa': 'ES', 'netherlands': 'NL', 'holland': 'NL', 'sweden': 'SE',
            'switzerland': 'CH', 'norway': 'NO', 'denmark': 'DK', 'finland': 'FI', 'poland': 'PL',
            'austria': 'AT', 'belgium': 'BE', 'ireland': 'IE', 'portugal': 'PT', 'greece': 'GR',
            'czech republic': 'CZ', 'czechia': 'CZ', 'the czech republic': 'CZ', 
            'hungary': 'HU', 'romania': 'RO', 'bulgaria': 'BG', 'croatia': 'HR',
            'serbia': 'RS', 'ukraine': 'UA', 'belarus': 'BY', 'slovakia': 'SK', 'slovenia': 'SI',
            'estonia': 'EE', 'latvia': 'LV', 'lithuania': 'LT', 'luxembourg': 'LU',
            'malta': 'MT', 'cyprus': 'CY', 'iceland': 'IS',
            
            # Asia
            'india': 'IN', 'china': 'CN', 'japan': 'JP', 'south korea': 'KR', 'korea': 'KR',
            'taiwan': 'TW', 'hong kong': 'HK', 'singapore': 'SG', 'malaysia': 'MY',
            'indonesia': 'ID', 'thailand': 'TH', 'vietnam': 'VN', 'philippines': 'PH',
            'pakistan': 'PK', 'bangladesh': 'BD', 'sri lanka': 'LK', 'nepal': 'NP',
            'israel': 'IL', 'turkey': 'TR', 'saudi arabia': 'SA', 'uae': 'AE',
            'united arab emirates': 'AE', 'iran': 'IR', 'iraq': 'IQ', 'qatar': 'QA',
            'kuwait': 'KW', 'oman': 'OM', 'jordan': 'JO', 'lebanon': 'LB',
            
            # Oceania
            'australia': 'AU', 'new zealand': 'NZ', 'fiji': 'FJ', 'papua new guinea': 'PG',
            
            # Africa
            'south africa': 'ZA', 'egypt': 'EG', 'morocco': 'MA', 'nigeria': 'NG',
            'kenya': 'KE', 'ghana': 'GH', 'ethiopia': 'ET', 'tanzania': 'TZ',
            'algeria': 'DZ', 'tunisia': 'TN',
            
            # South America
            'brazil': 'BR', 'brasil': 'BR', 'argentina': 'AR', 'chile': 'CL',
            'colombia': 'CO', 'peru': 'PE', 'venezuela': 'VE', 'ecuador': 'EC',
            'bolivia': 'BO', 'paraguay': 'PY', 'uruguay': 'UY',
            
            # Russia and former Soviet states
            'russia': 'RU', 'russian federation': 'RU', 'kazakhstan': 'KZ',
            'uzbekistan': 'UZ', 'turkmenistan': 'TM', 'kyrgyzstan': 'KG',
            'tajikistan': 'TJ', 'azerbaijan': 'AZ', 'armenia': 'AM', 'georgia': 'GE',
            'moldova': 'MD'
        }
        
        # Check for exact country name matches or country at the end
        for country_name, code in country_mapping.items():
            if country_name == location_lower or location_lower.endswith(f", {country_name}") or f" {country_name} " in f" {location_lower} ":
                return code
                
        # Check for "the" prefix in country names (e.g., "the Netherlands")
        for country_name, code in country_mapping.items():
            if f"the {country_name}" == location_lower or location_lower.endswith(f", the {country_name}"):
                return code
        
        # US States and territories mapping
        us_states = {
            'alabama': 'US', 'alaska': 'US', 'arizona': 'US', 'arkansas': 'US', 'california': 'US',
            'colorado': 'US', 'connecticut': 'US', 'delaware': 'US', 'florida': 'US', 'georgia': 'US',
            'hawaii': 'US', 'idaho': 'US', 'illinois': 'US', 'indiana': 'US', 'iowa': 'US',
            'kansas': 'US', 'kentucky': 'US', 'louisiana': 'US', 'maine': 'US', 'maryland': 'US',
            'massachusetts': 'US', 'michigan': 'US', 'minnesota': 'US', 'mississippi': 'US', 'missouri': 'US',
            'montana': 'US', 'nebraska': 'US', 'nevada': 'US', 'new hampshire': 'US', 'new jersey': 'US',
            'new mexico': 'US', 'new york': 'US', 'north carolina': 'US', 'north dakota': 'US', 'ohio': 'US',
            'oklahoma': 'US', 'oregon': 'US', 'pennsylvania': 'US', 'rhode island': 'US', 'south carolina': 'US',
            'south dakota': 'US', 'tennessee': 'US', 'texas': 'US', 'utah': 'US', 'vermont': 'US',
            'virginia': 'US', 'washington': 'US', 'west virginia': 'US', 'wisconsin': 'US', 'wyoming': 'US',
            'district of columbia': 'US', 'washington dc': 'US', 'washington d.c.': 'US', 'dc': 'US', 'd.c.': 'US',
            'puerto rico': 'US', 'guam': 'US', 'american samoa': 'US', 'virgin islands': 'US',
            'northern mariana islands': 'US'
        }
        
        # US State abbreviations
        us_state_abbr = {
            'al': 'US', 'ak': 'US', 'az': 'US', 'ar': 'US', 'ca': 'US', 'co': 'US', 'ct': 'US',
            'de': 'US', 'fl': 'US', 'ga': 'US', 'hi': 'US', 'id': 'US', 'il': 'US', 'in': 'US',
            'ia': 'US', 'ks': 'US', 'ky': 'US', 'la': 'US', 'me': 'US', 'md': 'US', 'ma': 'US',
            'mi': 'US', 'mn': 'US', 'ms': 'US', 'mo': 'US', 'mt': 'US', 'ne': 'US', 'nv': 'US',
            'nh': 'US', 'nj': 'US', 'nm': 'US', 'ny': 'US', 'nc': 'US', 'nd': 'US', 'oh': 'US',
            'ok': 'US', 'or': 'US', 'pa': 'US', 'ri': 'US', 'sc': 'US', 'sd': 'US', 'tn': 'US',
            'tx': 'US', 'ut': 'US', 'vt': 'US', 'va': 'US', 'wa': 'US', 'wv': 'US', 'wi': 'US',
            'wy': 'US', 'pr': 'US', 'gu': 'US', 'as': 'US', 'vi': 'US', 'mp': 'US'
        }
        
        # Major international cities mapping
        major_cities = {
            # North America - US
            'new york city': 'US', 'nyc': 'US', 'los angeles': 'US', 'chicago': 'US', 'houston': 'US',
            'phoenix': 'US', 'philadelphia': 'US', 'san antonio': 'US', 'san diego': 'US', 'dallas': 'US',
            'san jose': 'US', 'austin': 'US', 'jacksonville': 'US', 'fort worth': 'US', 'columbus': 'US',
            'san francisco': 'US', 'charlotte': 'US', 'indianapolis': 'US', 'seattle': 'US', 'denver': 'US',
            'boston': 'US', 'portland': 'US', 'las vegas': 'US', 'detroit': 'US', 'atlanta': 'US',
            'miami': 'US', 'minneapolis': 'US', 'pittsburgh': 'US', 'cincinnati': 'US', 'cleveland': 'US',
            'nashville': 'US', 'salt lake city': 'US', 'baltimore': 'US', 'brooklyn': 'US', 'manhattan': 'US',
            'queens': 'US', 'bronx': 'US', 'staten island': 'US', 'silicon valley': 'US', 'bay area': 'US',
            
            # North America - Canada
            'toronto': 'CA', 'montreal': 'CA', 'vancouver': 'CA', 'ottawa': 'CA', 'calgary': 'CA',
            'edmonton': 'CA', 'quebec city': 'CA', 'winnipeg': 'CA', 'hamilton': 'CA',
            
            # North America - Mexico
            'mexico city': 'MX', 'guadalajara': 'MX', 'monterrey': 'MX', 'puebla': 'MX',
            
            # Europe - UK
            'london': 'GB', 'manchester': 'GB', 'birmingham': 'GB', 'glasgow': 'GB', 'liverpool': 'GB',
            'edinburgh': 'GB', 'bristol': 'GB', 'cardiff': 'GB', 'belfast': 'GB', 'cambridge': 'GB',
            'oxford': 'GB', 'leeds': 'GB', 'newcastle': 'GB', 'sheffield': 'GB',
            
            # Europe - Germany
            'berlin': 'DE', 'munich': 'DE', 'hamburg': 'DE', 'cologne': 'DE', 'frankfurt': 'DE',
            'stuttgart': 'DE', 'düsseldorf': 'DE', 'dusseldorf': 'DE', 'dortmund': 'DE', 'essen': 'DE',
            'leipzig': 'DE', 'bremen': 'DE', 'dresden': 'DE', 'hannover': 'DE', 'nuremberg': 'DE',
            'duisburg': 'DE', 'bochum': 'DE', 'wuppertal': 'DE', 'bielefeld': 'DE', 'bonn': 'DE',
            'münster': 'DE', 'munster': 'DE', 'karlsruhe': 'DE', 'mannheim': 'DE', 'augsburg': 'DE',
            'wiesbaden': 'DE', 'gelsenkirchen': 'DE', 'mönchengladbach': 'DE', 'braunschweig': 'DE',
            'kiel': 'DE', 'chemnitz': 'DE', 'aachen': 'DE', 'halle': 'DE', 'magdeburg': 'DE',
            'freiburg': 'DE', 'krefeld': 'DE', 'lübeck': 'DE', 'oberhausen': 'DE', 'erfurt': 'DE',
            'mainz': 'DE', 'rostock': 'DE', 'kassel': 'DE', 'hagen': 'DE', 'hamm': 'DE',
            'saarbrücken': 'DE', 'mülheim': 'DE', 'potsdam': 'DE', 'ludwigshafen': 'DE',
            'oldenburg': 'DE', 'leverkusen': 'DE', 'osnabrück': 'DE', 'solingen': 'DE',
            
            # Europe - France
            'paris': 'FR', 'marseille': 'FR', 'lyon': 'FR', 'toulouse': 'FR', 'nice': 'FR',
            'nantes': 'FR', 'strasbourg': 'FR', 'montpellier': 'FR', 'bordeaux': 'FR', 'lille': 'FR',
            'rennes': 'FR', 'reims': 'FR', 'le havre': 'FR', 'saint-étienne': 'FR', 'toulon': 'FR',
            'grenoble': 'FR', 'dijon': 'FR', 'angers': 'FR', 'nîmes': 'FR', 'villeurbanne': 'FR',
            
            # Europe - Italy
            'rome': 'IT', 'roma': 'IT', 'milan': 'IT', 'milano': 'IT', 'naples': 'IT', 'napoli': 'IT',
            'turin': 'IT', 'torino': 'IT', 'palermo': 'IT', 'genoa': 'IT', 'genova': 'IT',
            'bologna': 'IT', 'florence': 'IT', 'firenze': 'IT', 'bari': 'IT', 'catania': 'IT',
            'venice': 'IT', 'venezia': 'IT', 'verona': 'IT', 'messina': 'IT', 'padua': 'IT',
            'padova': 'IT', 'trieste': 'IT', 'brescia': 'IT', 'prato': 'IT', 'taranto': 'IT',
            
            # Europe - Spain
            'madrid': 'ES', 'barcelona': 'ES', 'valencia': 'ES', 'seville': 'ES', 'sevilla': 'ES',
            'zaragoza': 'ES', 'málaga': 'ES', 'malaga': 'ES', 'murcia': 'ES', 'palma': 'ES',
            'las palmas': 'ES', 'bilbao': 'ES', 'alicante': 'ES', 'córdoba': 'ES', 'cordoba': 'ES',
            'valladolid': 'ES', 'vigo': 'ES', 'gijón': 'ES', 'gijon': 'ES', 'eixample': 'ES',
            
            # Europe - Netherlands
            'amsterdam': 'NL', 'rotterdam': 'NL', 'the hague': 'NL', 'utrecht': 'NL', 'eindhoven': 'NL',
            'tilburg': 'NL', 'groningen': 'NL', 'almere': 'NL', 'breda': 'NL', 'nijmegen': 'NL',
            
            # Europe - Other
            'prague': 'CZ', 'praha': 'CZ', 'warsaw': 'PL', 'warszawa': 'PL', 'budapest': 'HU',
            'vienna': 'AT', 'wien': 'AT', 'brussels': 'BE', 'bruxelles': 'BE', 'copenhagen': 'DK',
            'københavn': 'DK', 'stockholm': 'SE', 'helsinki': 'FI', 'oslo': 'NO', 'athens': 'GR',
            'lisbon': 'PT', 'lisboa': 'PT', 'dublin': 'IE', 'zurich': 'CH', 'zürich': 'CH',
            'geneva': 'CH', 'basel': 'CH', 'bern': 'CH', 'bratislava': 'SK', 'ljubljana': 'SI',
            'zagreb': 'HR', 'riga': 'LV', 'tallinn': 'EE', 'vilnius': 'LT', 'bucharest': 'RO',
            'sofia': 'BG', 'belgrade': 'RS', 'minsk': 'BY', 'kyiv': 'UA', 'kiev': 'UA',
            
            # Asia - China
            'beijing': 'CN', 'shanghai': 'CN', 'guangzhou': 'CN', 'shenzhen': 'CN', 'tianjin': 'CN',
            'wuhan': 'CN', 'chengdu': 'CN', 'chongqing': 'CN', 'nanjing': 'CN', 'xian': 'CN',
            'hangzhou': 'CN', 'shenyang': 'CN', 'qingdao': 'CN', 'jinan': 'CN', 'dalian': 'CN',
            'zhengzhou': 'CN', 'changsha': 'CN', 'fuzhou': 'CN', 'harbin': 'CN', 'suzhou': 'CN',
            
            # Asia - Japan
            'tokyo': 'JP', 'osaka': 'JP', 'yokohama': 'JP', 'nagoya': 'JP', 'sapporo': 'JP',
            'kobe': 'JP', 'kyoto': 'JP', 'fukuoka': 'JP', 'kawasaki': 'JP', 'saitama': 'JP',
            'hiroshima': 'JP', 'sendai': 'JP', 'kitakyushu': 'JP', 'chiba': 'JP', 'sakai': 'JP',
            
            # Asia - South Korea
            'seoul': 'KR', 'busan': 'KR', 'incheon': 'KR', 'daegu': 'KR', 'daejeon': 'KR',
            'gwangju': 'KR', 'suwon': 'KR', 'ulsan': 'KR', 'changwon': 'KR', 'goyang': 'KR',
            
            # Asia - India
            'mumbai': 'IN', 'delhi': 'IN', 'bangalore': 'IN', 'bengaluru': 'IN', 'hyderabad': 'IN',
            'ahmedabad': 'IN', 'chennai': 'IN', 'kolkata': 'IN', 'surat': 'IN', 'pune': 'IN',
            'jaipur': 'IN', 'lucknow': 'IN', 'kanpur': 'IN', 'nagpur': 'IN', 'indore': 'IN',
            'thane': 'IN', 'bhopal': 'IN', 'visakhapatnam': 'IN', 'pimpri-chinchwad': 'IN',
            'patna': 'IN', 'vadodara': 'IN', 'ghaziabad': 'IN', 'ludhiana': 'IN', 'agra': 'IN',
            
            # Asia - Other
            'singapore': 'SG', 'taipei': 'TW', 'hong kong': 'HK', 'bangkok': 'TH', 'kuala lumpur': 'MY',
            'jakarta': 'ID', 'manila': 'PH', 'ho chi minh city': 'VN', 'hanoi': 'VN', 'yangon': 'MM',
            'karachi': 'PK', 'lahore': 'PK', 'dhaka': 'BD', 'colombo': 'LK', 'kathmandu': 'NP',
            'tel aviv': 'IL', 'jerusalem': 'IL', 'istanbul': 'TR', 'ankara': 'TR', 'izmir': 'TR',
            'dubai': 'AE', 'abu dhabi': 'AE', 'riyadh': 'SA', 'jeddah': 'SA', 'doha': 'QA',
            'kuwait city': 'KW', 'muscat': 'OM', 'amman': 'JO', 'beirut': 'LB', 'tehran': 'IR',
            'baghdad': 'IQ',
            
            # Oceania
            'sydney': 'AU', 'melbourne': 'AU', 'brisbane': 'AU', 'perth': 'AU', 'adelaide': 'AU',
            'gold coast': 'AU', 'canberra': 'AU', 'newcastle': 'AU', 'wollongong': 'AU',
            'auckland': 'NZ', 'wellington': 'NZ', 'christchurch': 'NZ', 'hamilton': 'NZ',
            
            # Africa
            'cairo': 'EG', 'alexandria': 'EG', 'casablanca': 'MA', 'rabat': 'MA', 'tunis': 'TN',
            'algiers': 'DZ', 'lagos': 'NG', 'kano': 'NG', 'ibadan': 'NG', 'abuja': 'NG',
            'nairobi': 'KE', 'accra': 'GH', 'addis ababa': 'ET', 'dar es salaam': 'TZ',
            'johannesburg': 'ZA', 'cape town': 'ZA', 'durban': 'ZA', 'pretoria': 'ZA',
            'port elizabeth': 'ZA',
            
            # South America
            'são paulo': 'BR', 'sao paulo': 'BR', 'rio de janeiro': 'BR', 'brasília': 'BR',
            'brasilia': 'BR', 'salvador': 'BR', 'fortaleza': 'BR', 'belo horizonte': 'BR',
            'manaus': 'BR', 'curitiba': 'BR', 'recife': 'BR', 'porto alegre': 'BR',
            'buenos aires': 'AR', 'córdoba': 'AR', 'rosario': 'AR', 'mendoza': 'AR',
            'santiago': 'CL', 'valparaíso': 'CL', 'concepción': 'CL', 'lima': 'PE',
            'bogotá': 'CO', 'bogota': 'CO', 'medellín': 'CO', 'medellin': 'CO', 'cali': 'CO',
            'caracas': 'VE', 'maracaibo': 'VE', 'quito': 'EC', 'guayaquil': 'EC',
            'la paz': 'BO', 'santa cruz': 'BO', 'asunción': 'PY', 'asuncion': 'PY',
            'montevideo': 'UY',
            
            # Russia and former Soviet states
            'moscow': 'RU', 'saint petersburg': 'RU', 'st. petersburg': 'RU', 'novosibirsk': 'RU',
            'yekaterinburg': 'RU', 'nizhny novgorod': 'RU', 'kazan': 'RU', 'chelyabinsk': 'RU',
            'omsk': 'RU', 'samara': 'RU', 'rostov-on-don': 'RU', 'ufa': 'RU', 'krasnoyarsk': 'RU',
            'voronezh': 'RU', 'perm': 'RU', 'volgograd': 'RU', 'krasnodar': 'RU', 'saratov': 'RU',
            'almaty': 'KZ', 'nur-sultan': 'KZ', 'astana': 'KZ', 'tashkent': 'UZ', 'ashgabat': 'TM',
            'bishkek': 'KG', 'dushanbe': 'TJ', 'baku': 'AZ', 'yerevan': 'AM', 'tbilisi': 'GE',
            'chisinau': 'MD'
        }
        
        # Check for US state names in location
        for state, code in us_states.items():
            if state == location_lower or location_lower.endswith(f", {state}") or f" {state} " in f" {location_lower} ":
                return code
                
        # Check for US state abbreviations (only if they are at the end or after a comma to avoid false positives)
        state_abbr_pattern = r',\s*([A-Za-z]{2})$|\s+([A-Za-z]{2})$'
        match = re.search(state_abbr_pattern, location)
        if match:
            abbr = next((g.lower() for g in match.groups() if g is not None), None)
            if abbr in us_state_abbr:
                return 'US'
                
        # Check for major cities
        for city, code in major_cities.items():
            if city == location_lower or location_lower.startswith(f"{city},") or location_lower.startswith(f"{city} "):
                return code
                
        return None
        
    def _geocode_location(self, location: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Perform actual geocoding.
        
        Args:
            location: Preprocessed location string
            
        Returns:
            Tuple of (country_code, region)
        """
        try:
            # Initialize geocoder with increased timeout
            geolocator = Nominatim(user_agent="github_location_geocoder", timeout=10)
            
            # Try geocoding
            geo = geolocator.geocode(location, exactly_one=True, language='en')
            
            if geo and 'address' in geo.raw:
                address = geo.raw['address']
                country_code = address.get('country_code', '').upper()
                
                if country_code:
                    region = self._get_region_from_country_code(country_code)
                    return country_code, region
            
            return None, None
            
        except (GeocoderTimedOut, GeocoderUnavailable) as e:
            logger.warning(f"Geocoding error for '{location}': {e}")
            return None, None
        except Exception as e:
            logger.error(f"Unexpected error geocoding '{location}': {e}")
            return None, None
            
    def _get_region_from_country_code(self, country_code: str) -> Optional[str]:
        """
        Determine region based on country code.
        
        Args:
            country_code: ISO country code (2 letters)
            
        Returns:
            Region or None if country code is unknown
        """
        return self.country_to_region.get(country_code)
        
    def _extract_location_data(self, location: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Extract country codes and regions from location strings with geocoding.
        
        Args:
            location: Location string
            
        Returns:
            Tuple of (country_code, region)
        """
        if not location:
            return None, None
            
        # Check if location is already in cache
        if location in self.geocoding_cache:
            return self.geocoding_cache[location]
            
        # Preprocess location
        processed_location = self._preprocess_location(location)
        if not processed_location:
            return None, None
            
        # Try to extract country directly from text first
        country_code = self._extract_country_from_text(processed_location)
        if country_code:
            region = self._get_region_from_country_code(country_code)
            self.geocoding_cache[location] = (country_code, region)
            return country_code, region
            
        # If text-based extraction fails, try geocoding
        country_code, region = self._geocode_location(processed_location)
        
        # If geocoding fails, try with simplified location (e.g., just the last part after comma)
        if not country_code and ',' in processed_location:
            parts = [p.strip() for p in processed_location.split(',')]
            if len(parts) > 1:
                # Try with the last part first (often country or state)
                last_part = parts[-1]
                country_code = self._extract_country_from_text(last_part)
                if country_code:
                    region = self._get_region_from_country_code(country_code)
                else:
                    # Try geocoding with just the last part
                    country_code, region = self._geocode_location(last_part)
        
        # Save to cache
        self.geocoding_cache[location] = (country_code, region)
        return country_code, region
        
    def update_contributor_geocoding(self, session: Session, batch_size: int = 50, max_items: int = None) -> int:
        """
        Update geocoding for all contributors in the database.
        
        Args:
            session: Database session
            batch_size: Number of contributors per batch
            max_items: Maximum number of contributors to process (None for all)
            
        Returns:
            Number of updated contributors
        """
        total_updated = 0
        total_processed = 0
        
        # Count total contributors with location but missing country code
        count_query = session.query(Contributor).filter(
            Contributor.location.isnot(None),
            Contributor.country_code.is_(None)
        )
        total_count = count_query.count()
        
        if max_items:
            total_count = min(total_count, max_items)
            
        logger.info(f"Found {total_count} contributors with location but missing country code")
        
        # Process in batches
        query = session.query(Contributor).filter(
            Contributor.location.isnot(None),
            Contributor.country_code.is_(None)
        )
        
        if max_items:
            query = query.limit(max_items)
            
        for i, contributor in enumerate(query.yield_per(batch_size)):
            if contributor.location:
                country_code, region = self._extract_location_data(contributor.location)
                
                if country_code:
                    contributor.country_code = country_code
                    contributor.region = region
                    total_updated += 1
                    
            total_processed += 1
            
            # Log progress
            if total_processed % 100 == 0:
                logger.info(f"Processed {total_processed}/{total_count} contributors")
                session.commit()
                
        # Final commit
        session.commit()
        logger.info(f"Updated {total_updated}/{total_processed} contributors with country codes")
        
        return total_updated
        
    def update_organization_geocoding(self, session: Session, batch_size: int = 50, max_items: int = None) -> int:
        """
        Update geocoding for all organizations in the database.
        
        Args:
            session: Database session
            batch_size: Number of organizations per batch
            max_items: Maximum number of organizations to process (None for all)
            
        Returns:
            Number of updated organizations
        """
        total_updated = 0
        total_processed = 0
        
        # Count total organizations with location but missing country code
        count_query = session.query(Organization).filter(
            Organization.location.isnot(None),
            Organization.country_code.is_(None)
        )
        total_count = count_query.count()
        
        if max_items:
            total_count = min(total_count, max_items)
            
        logger.info(f"Found {total_count} organizations with location but missing country code")
        
        # Process in batches
        query = session.query(Organization).filter(
            Organization.location.isnot(None),
            Organization.country_code.is_(None)
        )
        
        if max_items:
            query = query.limit(max_items)
            
        for i, organization in enumerate(query.yield_per(batch_size)):
            if organization.location:
                country_code, region = self._extract_location_data(organization.location)
                
                if country_code:
                    organization.country_code = country_code
                    organization.region = region
                    total_updated += 1
                    
            total_processed += 1
            
            # Log progress
            if total_processed % 100 == 0:
                logger.info(f"Processed {total_processed}/{total_count} organizations")
                session.commit()
                
        # Final commit
        session.commit()
        logger.info(f"Updated {total_updated}/{total_processed} organizations with country codes")
        
        return total_updated
        
    def run_geocoding_update(self, batch_size: int = 50, max_items: int = None):
        """
        Run geocoding update for all contributors and organizations.
        
        Args:
            batch_size: Number of items per batch
            max_items: Maximum number of items to process (None for all)
        """
        try:
            with self.Session() as session:
                # Update contributors
                logger.info("Updating contributor geocoding...")
                updated_contributors = self.update_contributor_geocoding(
                    session, batch_size, max_items
                )
                
                # Update organizations
                logger.info("Updating organization geocoding...")
                updated_organizations = self.update_organization_geocoding(
                    session, batch_size, max_items
                )
                
                # Final stats
                logger.info(f"Geocoding update completed:")
                logger.info(f"  - Updated {updated_contributors} contributors")
                logger.info(f"  - Updated {updated_organizations} organizations")
                
                # Save final cache
                self._save_geocoding_cache()
                
        except Exception as e:
            logger.error(f"Error during geocoding update: {e}")
            raise


def main():
    """Main function for updating geocoding."""
    parser = argparse.ArgumentParser(description="Update location geocoding in GitHub database")
    
    parser.add_argument("--db-path", default="github_data.db",
                        help="Path to SQLite database (default: github_data.db)")
    
    parser.add_argument("--cache-file", default="geocoding_cache.json",
                        help="Path to geocoding cache file (default: geocoding_cache.json)")
                        
    parser.add_argument("--ignore-cache", action="store_true",
                        help="Ignore existing cache and perform fresh geocoding")
                        
    parser.add_argument("--batch-size", type=int, default=50,
                        help="Number of items to process in each batch (default: 50)")
                        
    parser.add_argument("--max-items", type=int, default=None,
                        help="Maximum number of items to process (default: all)")
    
    args = parser.parse_args()
    
    # Construct database URL
    db_path = args.db_path
    if not os.path.isabs(db_path):
        db_path = os.path.abspath(db_path)
    db_url = f"sqlite:///{db_path}"
    
    logger.info(f"Using database: {db_path}")
    
    # Initialize and run the geocoding updater
    updater = GeocodingUpdater(
        db_url=db_url,
        cache_file=args.cache_file,
        ignore_cache=args.ignore_cache
    )
    
    updater.run_geocoding_update(
        batch_size=args.batch_size,
        max_items=args.max_items
    )


if __name__ == "__main__":
    main()
