"""Data aggregation module for GitHub data."""

import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import csv
import os
from pathlib import Path

from sqlalchemy.orm import Session
from sqlalchemy import func, desc

from ..config import ETLConfig
from ..api.bigquery_api import BigQueryClient
from ..database.database import (
    Contributor, Organization, Repository, 
    OrganizationYearlyStats, CountryYearlyStats
)

logger = logging.getLogger(__name__)

class DataAggregator:
    """Aggregates GitHub data from BigQuery and GitHub API."""
    
    def __init__(self, config: ETLConfig, session: Session):
        """Initialize aggregator with configuration."""
        self.config = config
        self.session = session
        self.bigquery_client = BigQueryClient(config.bigquery)
    
    def aggregate_organization_stats(self, start_year: int = 2015, end_year: int = 2023) -> None:
        """
        Aggregate organization statistics by year.
        
        Args:
            start_year: Start year for aggregation
            end_year: End year for aggregation
        """
        logger.info(f"Aggregating organization statistics from {start_year} to {end_year}")
        
        # Optimierte BigQuery-Abfrage für Organisationsstatistiken
        # Reduzieren der abgefragten Spalten und Einschränkung des Zeitraums
        query = f"""
        SELECT
          EXTRACT(YEAR FROM created_at) AS year,
          repo.organization.login AS organization_login,
          COUNT(DISTINCT repo.id) AS number_repos,
          SUM(CASE WHEN type = 'ForkEvent' THEN 1 ELSE 0 END) AS forks,
          SUM(CASE WHEN type = 'WatchEvent' THEN 1 ELSE 0 END) AS stars,
          COUNT(DISTINCT actor.id) AS number_contributors
        FROM
          `githubarchive.day.events`
        WHERE
          repo.organization.login IS NOT NULL
          AND created_at BETWEEN '{start_year}-01-01' AND '{end_year}-12-31'
          AND _TABLE_SUFFIX BETWEEN '{start_year}0101' AND '{end_year}1231'
          -- Zusätzliche Filter, um die Datenmenge zu reduzieren
          AND (type = 'ForkEvent' OR type = 'WatchEvent' OR type = 'PushEvent')
        GROUP BY
          year, organization_login
        ORDER BY
          year, organization_login
        """
        
        try:
            # Führe die Abfrage mit Kostenlimit aus
            results = self.bigquery_client.query(
                query, 
                max_bytes=self.config.bigquery.max_bytes
            )
            
            # Verarbeite die Ergebnisse
            for row in results:
                # Finde die Organisation in der Datenbank
                org = self.session.query(Organization).filter_by(login=row.organization_login).first()
                if not org:
                    logger.warning(f"Organization {row.organization_login} not found in database")
                    continue
                
                # Erstelle oder aktualisiere OrganizationYearlyStats
                stats = self.session.query(OrganizationYearlyStats).filter_by(
                    year=row.year, organization_id=org.id
                ).first()
                
                if not stats:
                    stats = OrganizationYearlyStats(
                        year=row.year,
                        organization_id=org.id,
                        location=org.location,
                        country_code=org.country_code,
                        region=org.region
                    )
                
                # Aktualisiere die Statistiken
                stats.forks = row.forks
                stats.stars = row.stars
                stats.number_repos = row.number_repos
                stats.number_commits = 0  # Wird später aktualisiert
                stats.number_contributors = row.number_contributors
                
                self.session.add(stats)
            
            # Commits separat abfragen, um die Abfragegröße zu reduzieren
            self._update_organization_commits(start_year, end_year)
            
            self.session.commit()
            logger.info(f"Successfully aggregated organization statistics")
            
        except Exception as e:
            logger.error(f"Error aggregating organization statistics: {e}")
            self.session.rollback()
            raise
    
    def _update_organization_commits(self, start_year: int, end_year: int) -> None:
        """
        Update organization commit counts in a separate query to reduce query size.
        
        Args:
            start_year: Start year for aggregation
            end_year: End year for aggregation
        """
        logger.info(f"Updating organization commit counts from {start_year} to {end_year}")
        
        # Optimierte Abfrage nur für Commits
        query = f"""
        SELECT
          EXTRACT(YEAR FROM created_at) AS year,
          repo.organization.login AS organization_login,
          COUNT(DISTINCT payload.commits.sha) AS number_commits
        FROM
          `githubarchive.day.events`
        WHERE
          repo.organization.login IS NOT NULL
          AND created_at BETWEEN '{start_year}-01-01' AND '{end_year}-12-31'
          AND _TABLE_SUFFIX BETWEEN '{start_year}0101' AND '{end_year}1231'
          AND type = 'PushEvent'
          AND payload.commits IS NOT NULL
        GROUP BY
          year, organization_login
        ORDER BY
          year, organization_login
        """
        
        try:
            # Führe die Abfrage mit Kostenlimit aus
            results = self.bigquery_client.query(
                query, 
                max_bytes=self.config.bigquery.max_bytes
            )
            
            # Verarbeite die Ergebnisse
            for row in results:
                # Finde die Organisation in der Datenbank
                org = self.session.query(Organization).filter_by(login=row.organization_login).first()
                if not org:
                    continue
                
                # Aktualisiere die Commits
                stats = self.session.query(OrganizationYearlyStats).filter_by(
                    year=row.year, organization_id=org.id
                ).first()
                
                if stats:
                    stats.number_commits = row.number_commits
                    self.session.add(stats)
            
            self.session.commit()
            logger.info(f"Successfully updated organization commit counts")
            
        except Exception as e:
            logger.error(f"Error updating organization commit counts: {e}")
            self.session.rollback()
            raise
    
    def aggregate_country_stats(self, start_year: int = 2015, end_year: int = 2023) -> None:
        """
        Aggregate country statistics by year.
        
        Args:
            start_year: Start year for aggregation
            end_year: End year for aggregation
        """
        logger.info(f"Aggregating country statistics from {start_year} to {end_year}")
        
        # Wir können dies entweder direkt über BigQuery machen oder aus unseren bereits
        # aggregierten Organisationsdaten ableiten
        
        # Methode 1: Aus OrganizationYearlyStats ableiten
        for year in range(start_year, end_year + 1):
            # Abfrage für jedes Land und Jahr
            country_stats = self.session.query(
                OrganizationYearlyStats.country_code,
                OrganizationYearlyStats.region,
                func.sum(OrganizationYearlyStats.forks).label('forks'),
                func.sum(OrganizationYearlyStats.stars).label('stars'),
                func.sum(OrganizationYearlyStats.number_repos).label('number_repos'),
                func.sum(OrganizationYearlyStats.number_commits).label('number_commits'),
                func.count(OrganizationYearlyStats.organization_id.distinct()).label('organization_count'),
                func.sum(OrganizationYearlyStats.number_contributors).label('contributor_count')
            ).filter(
                OrganizationYearlyStats.year == year,
                OrganizationYearlyStats.country_code != None  # Nur Einträge mit Ländercode
            ).group_by(
                OrganizationYearlyStats.country_code,
                OrganizationYearlyStats.region
            ).all()
            
            for stats in country_stats:
                if not stats.country_code:
                    continue
                    
                # Erstelle oder aktualisiere CountryYearlyStats
                country_stat = self.session.query(CountryYearlyStats).filter_by(
                    year=year, country_code=stats.country_code
                ).first()
                
                if not country_stat:
                    country_stat = CountryYearlyStats(
                        year=year,
                        country_code=stats.country_code,
                        region=stats.region
                    )
                
                # Aktualisiere die Statistiken
                country_stat.forks = stats.forks
                country_stat.stars = stats.stars
                country_stat.number_repos = stats.number_repos
                country_stat.number_commits = stats.number_commits
                country_stat.number_organizations = stats.organization_count
                country_stat.number_contributors = stats.contributor_count
                
                self.session.add(country_stat)
            
            self.session.commit()
            logger.info(f"Successfully aggregated country statistics for year {year}")
    
    def export_organization_stats(self, output_file: str) -> None:
        """
        Export organization statistics to CSV.
        
        Args:
            output_file: Path to output CSV file
        """
        logger.info(f"Exporting organization statistics to {output_file}")
        
        # Stelle sicher, dass das Verzeichnis existiert
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        data = self.session.query(
            OrganizationYearlyStats.year,
            Organization.login.label('organization'),
            OrganizationYearlyStats.location,
            OrganizationYearlyStats.country_code,
            OrganizationYearlyStats.region,
            OrganizationYearlyStats.forks,
            OrganizationYearlyStats.stars,
            OrganizationYearlyStats.number_repos,
            OrganizationYearlyStats.number_commits,
            OrganizationYearlyStats.number_contributors
        ).join(
            Organization, 
            OrganizationYearlyStats.organization_id == Organization.id
        ).order_by(
            OrganizationYearlyStats.year,
            Organization.login
        ).all()
        
        with open(output_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                'Jahr', 'Organization', 'Location', 'Country', 'Region',
                'Forks', 'Stars', 'Number Repos', 'Number Commits', 'Contributors'
            ])
            for row in data:
                writer.writerow([
                    row.year, row.organization, row.location, row.country_code, row.region,
                    row.forks, row.stars, row.number_repos, row.number_commits,
                    row.number_contributors
                ])
        
        logger.info(f"Successfully exported organization statistics to {output_file}")
    
    def export_country_stats(self, output_file: str) -> None:
        """
        Export country statistics to CSV.
        
        Args:
            output_file: Path to output CSV file
        """
        logger.info(f"Exporting country statistics to {output_file}")
        
        # Stelle sicher, dass das Verzeichnis existiert
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        data = self.session.query(
            CountryYearlyStats.year,
            CountryYearlyStats.country_code,
            CountryYearlyStats.region,
            CountryYearlyStats.forks,
            CountryYearlyStats.stars,
            CountryYearlyStats.number_repos,
            CountryYearlyStats.number_commits,
            CountryYearlyStats.number_organizations,
            CountryYearlyStats.number_contributors
        ).order_by(
            CountryYearlyStats.year,
            CountryYearlyStats.country_code
        ).all()
        
        with open(output_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                'Jahr', 'Country', 'Region', 'Forks', 'Stars', 'Number Repos', 
                'Number Commits', 'Organization Count', 'Contributor Count'
            ])
            for row in data:
                writer.writerow([
                    row.year, row.country_code, row.region, row.forks, row.stars, 
                    row.number_repos, row.number_commits, row.number_organizations,
                    row.number_contributors
                ])
        
        logger.info(f"Successfully exported country statistics to {output_file}")
    
    def export_data(self, output_dir: str) -> None:
        """
        Export aggregated data to CSV files.
        
        Args:
            output_dir: Directory to save CSV files
        """
        logger.warning("The export_data method is deprecated. Use export_organization_stats and export_country_stats instead.")
        
        os.makedirs(output_dir, exist_ok=True)
        
        # Export OrganizationYearlyStats
        self.export_organization_stats(os.path.join(output_dir, "organization_stats.csv"))
        
        # Export CountryYearlyStats
        self.export_country_stats(os.path.join(output_dir, "country_stats.csv"))
        
        logger.info(f"Data exported to {output_dir}")
    
    def _extract_country_code(self, location: Optional[str]) -> Optional[str]:
        """
        Extract country code from location string.
        This is a simple implementation and could be improved with geocoding.
        
        Args:
            location: Location string
            
        Returns:
            Two-letter country code or None
        """
        if not location:
            return None
            
        # Einfache Mapping-Tabelle für häufige Standorte
        location_mapping = {
            'san francisco': 'US',
            'new york': 'US',
            'seattle': 'US',
            'london': 'GB',
            'berlin': 'DE',
            'paris': 'FR',
            'tokyo': 'JP',
            'sydney': 'AU',
            'toronto': 'CA',
            'bangalore': 'IN',
            'beijing': 'CN',
            'mumbai': 'IN',
            'sao paulo': 'BR',
        }
        
        location_lower = location.lower()
        
        # Direktes Mapping für Städte
        for city, country in location_mapping.items():
            if city in location_lower:
                return country
                
        # Ländercodes (einfache Heuristik)
        country_codes = {
            'usa': 'US',
            'united states': 'US',
            'uk': 'GB',
            'united kingdom': 'GB',
            'germany': 'DE',
            'deutschland': 'DE',
            'france': 'FR',
            'japan': 'JP',
            'australia': 'AU',
            'canada': 'CA',
            'india': 'IN',
            'china': 'CN',
            'brazil': 'BR',
            'brasil': 'BR',
        }
        
        for country_name, code in country_codes.items():
            if country_name in location_lower:
                return code
                
        return None
