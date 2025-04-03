"""
Optimierter Geocoding-Service für GitHub-Standortdaten.

Ermöglicht die effiziente Extraktion von Länder- und Regionsinformationen
aus unstrukturierten Standortangaben von GitHub-Benutzern und Organisationen.
"""

import os
import json
import logging
import re
from typing import Dict, Any, Optional, Tuple
import time
from pathlib import Path

import requests
from geopy.geocoders import Nominatim
import pycountry

logger = logging.getLogger(__name__)

class GeocodingService:
    """
    Service für die Extraktion geografischer Informationen aus Standortangaben.
    
    Verwendet einen mehrstufigen Ansatz:
    1. Pattern-Matching für gängige Formate
    2. Lokaler Cache für bereits bekannte Standorte
    3. Nominatim/OpenStreetMap-API für unbekannte Standorte
    """
    
    def __init__(self, cache_file: str = "geocoding_cache.json", user_agent: str = "github_geocoder"):
        """
        Initialisiert den Geocoding-Service.
        
        Args:
            cache_file: Pfad zur Cache-Datei
            user_agent: User-Agent für Nominatim-API
        """
        self.cache_file = cache_file
        self.cache = self._load_cache()
        self.geolocator = Nominatim(user_agent=user_agent)
        
        # Land-Lookup-Tables
        self.country_names = {country.name.lower(): country.alpha_2 for country in pycountry.countries}
        self.country_codes = {country.alpha_2.lower(): country.alpha_2 for country in pycountry.countries}
        self.country_codes.update({country.alpha_3.lower(): country.alpha_2 for country in pycountry.countries})
        
        # Regionen für Kontinente
        self.continent_map = {
            "europe": "Europe",
            "asia": "Asia",
            "africa": "Africa",
            "north america": "North America", 
            "south america": "South America",
            "australia": "Oceania",
            "oceania": "Oceania",
            "antarctica": "Antarctica"
        }
        
        # Häufige Städte und ihre Länder direkt mappen
        self.common_cities = {
            "san francisco": ("US", "North America", 37.7749, -122.4194),
            "new york": ("US", "North America", 40.7128, -74.0060),
            "london": ("GB", "Europe", 51.5074, -0.1278),
            "berlin": ("DE", "Europe", 52.5200, 13.4050),
            "paris": ("FR", "Europe", 48.8566, 2.3522),
            "tokyo": ("JP", "Asia", 35.6762, 139.6503),
            "beijing": ("CN", "Asia", 39.9042, 116.4074),
            "sydney": ("AU", "Oceania", -33.8688, 151.2093),
            "mumbai": ("IN", "Asia", 19.0760, 72.8777)
        }
        
    def _load_cache(self) -> Dict[str, Dict[str, Any]]:
        """Lädt den Geocoding-Cache aus der Datei."""
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Fehler beim Laden des Geocoding-Caches: {e}")
        return {}
    
    def _save_cache(self) -> None:
        """Speichert den Geocoding-Cache in der Datei."""
        try:
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(self.cache, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Fehler beim Speichern des Geocoding-Caches: {e}")
    
    def get_location_info(self, location: str) -> Dict[str, Any]:
        """
        Extrahiert geografische Informationen aus einer Standortangabe.
        
        Args:
            location: Standortangabe als String
            
        Returns:
            Dict mit country_code, region, latitude und longitude
        """
        if not location or not isinstance(location, str):
            return {"country_code": None, "region": None, "latitude": None, "longitude": None}
        
        # Normalisieren und im Cache nachschlagen
        location = location.strip().lower()
        if location in self.cache:
            return self.cache[location]
        
        # Ergebnis-Dict initialisieren
        result = {"country_code": None, "region": None, "latitude": None, "longitude": None}
        
        # 1. Direkte Übereinstimmung mit Land oder Kontinent
        if location in self.country_names:
            result["country_code"] = self.country_names[location]
            result["region"] = self._get_region_for_country(result["country_code"])
            
        # 2. Direkte Übereinstimmung mit Ländercode
        elif location in self.country_codes:
            result["country_code"] = self.country_codes[location]
            result["region"] = self._get_region_for_country(result["country_code"])
            
        # 3. Bekannte Stadt
        elif location in self.common_cities:
            country_code, region, lat, lon = self.common_cities[location]
            result["country_code"] = country_code
            result["region"] = region
            result["latitude"] = lat
            result["longitude"] = lon
            
        # 4. Kontinent prüfen
        elif location in self.continent_map:
            result["region"] = self.continent_map[location]
            
        # 5. Muster für "Stadt, Land" oder "Stadt, Ländercode"
        elif "," in location:
            parts = [part.strip() for part in location.split(",")]
            if len(parts) >= 2:
                # Letzten Teil als mögliches Land oder Code betrachten
                potential_country = parts[-1].lower()
                
                if potential_country in self.country_names:
                    result["country_code"] = self.country_names[potential_country]
                    result["region"] = self._get_region_for_country(result["country_code"])
                elif potential_country in self.country_codes:
                    result["country_code"] = self.country_codes[potential_country]
                    result["region"] = self._get_region_for_country(result["country_code"])
                    
        # 6. Wenn noch nichts gefunden, Nominatim/OpenStreetMap abfragen
        if not result["country_code"] and not result["region"]:
            try:
                # Geocoding-API aufrufen
                geocode_result = self.geolocator.geocode(location, exactly_one=True, language="en")
                
                if geocode_result:
                    # Land aus den Adressdaten extrahieren
                    country = None
                    if 'address' in geocode_result.raw:
                        address = geocode_result.raw['address']
                        if 'country_code' in address:
                            country_code = address['country_code'].upper()
                            if len(country_code) == 2:
                                result["country_code"] = country_code
                                result["region"] = self._get_region_for_country(country_code)
                    
                    # Koordinaten speichern
                    result["latitude"] = geocode_result.latitude
                    result["longitude"] = geocode_result.longitude
                    
                # Pause, um API-Limits zu respektieren
                time.sleep(1)
                
            except Exception as e:
                logger.warning(f"Fehler beim Geocoding von '{location}': {e}")
        
        # Ergebnis im Cache speichern
        self.cache[location] = result
        self._save_cache()
        
        return result
    
    def _get_region_for_country(self, country_code: str) -> str:
        """
        Ermittelt die Region (Kontinent) für einen Ländercode.
        
        Args:
            country_code: Zweistelliger ISO-Ländercode
            
        Returns:
            Regions-/Kontinentname oder None
        """
        # Einfache Zuordnung von Ländern zu Kontinenten
        if not country_code:
            return None
            
        europe = ["AL", "AD", "AT", "BE", "BA", "BG", "HR", "CY", "CZ", "DK", "EE", 
                 "FI", "FR", "DE", "GR", "HU", "IS", "IE", "IT", "LV", "LI", "LT", 
                 "LU", "MT", "MC", "ME", "NL", "MK", "NO", "PL", "PT", "RO", "RS", 
                 "SK", "SI", "ES", "SE", "CH", "GB", "VA"]
        
        asia = ["AF", "AM", "AZ", "BH", "BD", "BT", "BN", "KH", "CN", "GE", "IN", 
               "ID", "IR", "IQ", "IL", "JP", "JO", "KZ", "KW", "KG", "LA", "LB", 
               "MY", "MV", "MN", "MM", "NP", "KP", "OM", "PK", "PS", "PH", "QA", 
               "SA", "SG", "KR", "LK", "SY", "TW", "TJ", "TH", "TR", "TM", "AE", 
               "UZ", "VN", "YE"]
        
        africa = ["DZ", "AO", "BJ", "BW", "BF", "BI", "CM", "CV", "CF", "TD", "KM", 
                 "CD", "DJ", "EG", "GQ", "ER", "SZ", "ET", "GA", "GM", "GH", "GN", 
                 "GW", "CI", "KE", "LS", "LR", "LY", "MG", "MW", "ML", "MR", "MU", 
                 "MA", "MZ", "NA", "NE", "NG", "CG", "RW", "ST", "SN", "SC", "SL", 
                 "SO", "ZA", "SS", "SD", "TZ", "TG", "TN", "UG", "ZM", "ZW"]
        
        north_america = ["AG", "BS", "BB", "BZ", "CA", "CR", "CU", "DM", "DO", "SV", 
                         "GD", "GT", "HT", "HN", "JM", "MX", "NI", "PA", "KN", "LC", 
                         "VC", "TT", "US"]
        
        south_america = ["AR", "BO", "BR", "CL", "CO", "EC", "GY", "PY", "PE", "SR", 
                         "UY", "VE"]
        
        oceania = ["AU", "FJ", "KI", "MH", "FM", "NR", "NZ", "PW", "PG", "WS", "SB", 
                  "TO", "TV", "VU"]
        
        if country_code in europe:
            return "Europe"
        elif country_code in asia:
            return "Asia"
        elif country_code in africa:
            return "Africa"
        elif country_code in north_america:
            return "North America"
        elif country_code in south_america:
            return "South America"
        elif country_code in oceania:
            return "Oceania"
        else:
            return None
    
    def batch_process_locations(self, locations: Dict[str, str]) -> Dict[str, Dict[str, Any]]:
        """
        Verarbeitet mehrere Standorte gleichzeitig.
        
        Args:
            locations: Dict mit ID als Schlüssel und Standort als Wert
            
        Returns:
            Dict mit ID als Schlüssel und Geocoding-Ergebnissen als Werte
        """
        results = {}
        
        for entity_id, location in locations.items():
            if not location:
                continue
                
            results[entity_id] = self.get_location_info(location)
            
            # Fortschritt loggen
            if len(results) % 50 == 0:
                logger.info(f"Geocoding-Fortschritt: {len(results)}/{len(locations)}")
        
        return results
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """
        Gibt Statistiken über den Geocoding-Cache zurück.
        
        Returns:
            Dict mit Statistiken zum Cache
        """
        if not self.cache:
            return {
                "total_entries": 0,
                "with_country_code": 0,
                "with_region": 0,
                "with_coordinates": 0
            }
            
        with_country = sum(1 for entry in self.cache.values() if entry.get("country_code"))
        with_region = sum(1 for entry in self.cache.values() if entry.get("region"))
        with_coords = sum(1 for entry in self.cache.values() 
                         if entry.get("latitude") is not None and entry.get("longitude") is not None)
        
        return {
            "total_entries": len(self.cache),
            "with_country_code": with_country,
            "with_region": with_region,
            "with_coordinates": with_coords,
            "coverage_percentage": {
                "country": round(with_country / len(self.cache) * 100, 2),
                "region": round(with_region / len(self.cache) * 100, 2),
                "coordinates": round(with_coords / len(self.cache) * 100, 2)
            }
        }
