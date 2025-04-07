#!/usr/bin/env python3
"""
Repository Collector - Systematic collection of GitHub repositories.

This module implements an efficient strategy for systematically collecting
GitHub repositories using a time-based query approach for repositories with
at least 10 stars created since 2014.
"""

import os
import logging
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple, Optional

from github_database.database.database import GitHubDatabase
from github_database.api.github_api import GitHubAPIClient
from github_database.etl.orchestrator import ETLOrchestrator

logger = logging.getLogger(__name__)

class RepositoryCollector:
    """
    Collects GitHub repositories systematically.
    
    Implements a time-based search strategy to work around API limits
    and ensure that all relevant repositories are captured.
    """
    
    def __init__(self, github_client=None, db=None, state_file: str = "collection_state.json"):
        """
        Initialize the repository collector.
        
        Args:
            github_client: GitHub API client instance
            db: Database connection
            state_file: File to store collection progress
        """
        self.github_client = github_client
        self.db = db
        self.state_file = state_file
        
        # Load existing progress if orchestrator is provided
        if github_client and db:
            self.progress = self._load_collection_progress()
    
    def _load_collection_progress(self) -> Dict[str, Any]:
        """
        Load collection progress from state file.
        
        Returns:
            Dict with current collection progress
        """
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Error loading progress: {e}")
        
        # Default progress if no file exists
        return {
            "last_updated": datetime.now().isoformat(),
            "completed_periods": [],
            "total_repositories": 0,
            "last_period_processed": "2014-01",
            "stats": {
                "by_year": {}
            }
        }
    
    def _save_collection_progress(self):
        """Save current collection progress."""
        try:
            # Update timestamp
            self.progress["last_updated"] = datetime.now().isoformat()
            
            with open(self.state_file, 'w') as f:
                json.dump(self.progress, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving progress: {e}")
    
    def _is_period_processed(self, period: str) -> bool:
        """
        Check if a time period has already been processed.
        
        Args:
            period: Time period (YYYY-MM-DD-DD or YYYY-MM)
            
        Returns:
            True if the period has already been processed
        """
        return period in self.progress["completed_periods"]
    
    def _mark_period_processed(self, period: str, num_repositories: int):
        """
        Mark a time period as processed and update statistics.
        
        Args:
            period: Time period (YYYY-MM-DD-DD or YYYY-MM)
            num_repositories: Number of repositories found
        """
        if period not in self.progress["completed_periods"]:
            self.progress["completed_periods"].append(period)
        
        # Update statistics
        self.progress["total_repositories"] += num_repositories
        self.progress["last_period_processed"] = period
        
        # By year
        if "-" in period:
            parts = period.split("-")
            if len(parts) >= 2:
                year = parts[0]
                if year not in self.progress["stats"]["by_year"]:
                    self.progress["stats"]["by_year"][year] = 0
                self.progress["stats"]["by_year"][year] += num_repositories
        
        # Save progress
        self._save_collection_progress()
    
    def _get_next_month(self, period: str) -> str:
        """
        Calculate the first day of the next month.
        
        Args:
            period: Time period in format "YYYY-MM"
            
        Returns:
            Date of the first day of the next month in format "YYYY-MM-DD"
        """
        year, month = map(int, period.split('-'))
        if month == 12:
            return f"{year+1}-01-01"
        else:
            return f"{year}-{month+1:02d}-01"
    
    def collect_repositories_by_time(self, min_stars: int = 10, 
                                    start_year: int = 2014,
                                    total_limit: Optional[int] = None,
                                    resume: bool = True) -> int:
        """
        Systematically collect repositories using a time-based strategy.
        
        This method breaks down the repository collection into monthly periods
        to avoid GitHub API search limitations. It queries repositories created
        in each month, processes them through the ETL pipeline, and stores the
        results in the database.
        
        Args:
            min_stars: Minimum number of stars for repositories
            start_year: Starting year for collection (default: 2014)
            total_limit: Maximum number of repositories to collect (None for unlimited)
            resume: Whether to resume from the last processed period
            
        Returns:
            Number of collected repositories
        """
        logger.info(f"Starting systematic repository collection from year {start_year}")
        
        # Initialize counters
        total_collected = 0
        current_year = datetime.now().year
        current_month = datetime.now().month
        
        # Determine starting point
        if resume and self.progress["last_period_processed"]:
            last_period = self.progress["last_period_processed"]
            start_year, start_month = map(int, last_period.split('-'))
            
            # Move to next month
            if start_month == 12:
                start_year += 1
                start_month = 1
            else:
                start_month += 1
                
            if start_year > current_year:
                logger.info("Already processed all periods up to current date")
                return 0
        else:
            start_month = 1
        
        # Process each month from start_year to current date
        for year in range(start_year, current_year + 1):
            # Determine start and end months for this year
            start_month_in_year = 1
            end_month_in_year = 12
            
            if year == start_year:
                start_month_in_year = start_month
            if year == current_year:
                end_month_in_year = current_month
            
            for month in range(start_month_in_year, end_month_in_year + 1):
                # Break each month into three 10-day periods (or appropriate divisions)
                # Period 1: Day 1-10
                # Period 2: Day 11-20
                # Period 3: Day 21-end of month
                days_in_month = 31  # Default for most months
                if month in [4, 6, 9, 11]:
                    days_in_month = 30
                elif month == 2:
                    # Simple leap year check
                    if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0):
                        days_in_month = 29
                    else:
                        days_in_month = 28
                
                # Define the periods within the month
                periods = [
                    (1, 10),
                    (11, 20),
                    (21, days_in_month)
                ]
                
                for day_start, day_end in periods:
                    period = f"{year}-{month:02d}-{day_start:02d}-{day_end:02d}"
                    
                    # Skip if already processed
                    if self._is_period_processed(period):
                        logger.info(f"Period {period} already processed, skipping")
                        continue
                    
                    start_date = f"{year}-{month:02d}-{day_start:02d}"
                    if day_end == days_in_month and month == 12 and day_end == 31:
                        # For the last day of the year, use the first day of next year as end date
                        end_date = f"{year+1}-01-01"
                    elif day_end == days_in_month:
                        # For the last day of other months, use the first day of next month
                        next_month = month + 1 if month < 12 else 1
                        next_year = year if month < 12 else year + 1
                        end_date = f"{next_year}-{next_month:02d}-01"
                    else:
                        # For periods within a month, use the next day as end date
                        end_date = f"{year}-{month:02d}-{day_end+1:02d}"
                    
                    query = f"stars:>={min_stars} created:{start_date}..{end_date}"
                    logger.info(f"Collecting repositories for period {period} with query: {query}")
                    
                    # Search for repositories in this time period
                    try:
                        repositories = self.github_client.search_repositories(
                            query=query, 
                            sort="stars",
                            order="desc",
                            per_page=100
                        )
                        
                        # Process repositories
                        num_processed = 0
                        for repo in repositories:
                            # Check if repository already exists in the database
                            existing_repo = self.db.get_repository_by_id(repo['id'])
                            if existing_repo:
                                logger.debug(f"Repository {repo['full_name']} already in database, skipping")
                                continue
                            
                            # Process this repository through the ETL pipeline
                            try:
                                self.db.add_repository(repo)
                                num_processed += 1
                                total_collected += 1
                                logger.info(f"Processed repository: {repo['full_name']} ({num_processed} in period, {total_collected} total)")
                                
                                # Check if we've reached the limit
                                if total_limit and total_collected >= total_limit:
                                    logger.info(f"Reached collection limit of {total_limit} repositories")
                                    self._mark_period_processed(period, num_processed)
                                    return total_collected
                            except Exception as e:
                                logger.error(f"Error processing repository {repo['full_name']}: {e}")
                        
                        # Mark period as processed
                        logger.info(f"Completed period {period}, processed {num_processed} repositories")
                        self._mark_period_processed(period, num_processed)
                        
                    except Exception as e:
                        logger.error(f"Error searching repositories for period {period}: {e}")
        
        logger.info(f"Completed repository collection, collected {total_collected} repositories in total")
        return total_collected

    def collect_repositories(self, start_date, end_date, limit=None, min_stars=100):
        """
        Collect repositories created between start_date and end_date.
        
        Args:
            start_date: Start date for repository creation
            end_date: End date for repository creation
            limit: Maximum number of repositories to collect (None for unlimited)
            min_stars: Minimum number of stars for repositories
            
        Returns:
            List of collected Repository objects
        """
        # Format dates for the GitHub API query
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")
        
        logger.info(f"Collecting repositories created between {start_date_str} and {end_date_str}")
        logger.info(f"Minimum stars: {min_stars}")
        
        # For large time ranges, break down into smaller periods
        # to work around GitHub API's 1000 results limitation
        collected_repos = []
        total_collected = 0
        
        # Calculate the time difference in days
        from datetime import timedelta
        time_diff = (end_date - start_date).days
        
        # Determine optimal period size based on time range
        # For longer periods, use larger chunks to reduce API calls
        if time_diff > 365:  # More than a year
            period_size = 10  # Reduced from 30 to 10 days chunks
        elif time_diff > 90:  # More than 3 months
            period_size = 5   # Reduced from 15 to 5 days chunks
        elif time_diff > 30:  # More than a month
            period_size = 2   # Reduced from 7 to 2 days chunks
        else:
            period_size = 1   # Reduced from 3 to 1 day chunks for short periods
        
        # If the time range is large or we expect more than 1000 results, break it down
        if time_diff > period_size or (limit is None or limit > 1000):
            logger.info(f"Large time range detected ({time_diff} days). Breaking down into {period_size}-day periods.")
            
            # Calculate total number of periods for progress tracking
            total_periods = (time_diff + period_size - 1) // period_size
            current_period = 0
            
            # Break down by periods
            current_start = start_date
            while current_start < end_date:
                current_period += 1
                
                # Calculate end of this period (period_size days or end_date, whichever comes first)
                current_end = min(current_start + timedelta(days=period_size), end_date)
                
                # Determine limit for this period
                period_limit = None
                if limit is not None:
                    remaining_limit = limit - total_collected
                    if remaining_limit <= 0:
                        break
                    period_limit = remaining_limit
                
                # Show progress information
                progress = (current_period / total_periods) * 100
                logger.info(f"Processing period {current_period}/{total_periods} ({progress:.1f}%): " +
                           f"{current_start.strftime('%Y-%m-%d')} to {current_end.strftime('%Y-%m-%d')}")
                
                # Collect repositories for this period with retry logic
                max_retries = 3
                for retry in range(max_retries):
                    try:
                        period_repos = self._collect_repositories_for_period(
                            current_start, 
                            current_end, 
                            period_limit,
                            min_stars
                        )
                        break
                    except Exception as e:
                        if retry < max_retries - 1:
                            logger.warning(f"Error collecting repositories for period, retrying ({retry+1}/{max_retries}): {e}")
                            import time
                            time.sleep(5 * (retry + 1))  # Exponential backoff
                        else:
                            logger.error(f"Failed to collect repositories for period after {max_retries} attempts: {e}")
                            period_repos = []
                
                collected_repos.extend(period_repos)
                total_collected += len(period_repos)
                
                # Show detailed progress
                logger.info(f"Collected {len(period_repos)} repositories in period {current_period}. " +
                           f"Total so far: {total_collected}" +
                           (f"/{limit}" if limit is not None else ""))
                
                # Check if we've reached the overall limit
                if limit is not None and total_collected >= limit:
                    logger.info(f"Reached the specified limit of {limit} repositories")
                    break
                
                # Move to next period
                current_start = current_end
        else:
            # For smaller time ranges, collect directly
            collected_repos = self._collect_repositories_for_period(start_date, end_date, limit, min_stars)
            total_collected = len(collected_repos)
        
        logger.info(f"Total repositories collected: {total_collected}")
        return collected_repos
    
    def _collect_repositories_for_period(self, start_date, end_date, limit=None, min_stars=100):
        """
        Collect repositories for a specific time period.
        
        Args:
            start_date: Start date for repository creation
            end_date: End date for repository creation
            limit: Maximum number of repositories to collect
            min_stars: Minimum number of stars for repositories
            
        Returns:
            List of collected Repository objects
        """
        # Format dates for the GitHub API query
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")
        
        # Set up the search query for repositories
        query = f"created:{start_date_str}..{end_date_str} stars:>={min_stars}"
        
        # Use the API client to search repositories with pagination
        repos_data = self.github_client.search_repositories(
            query, 
            per_page=100,  # Maximum per page
            max_results=limit  # Total limit (None for all available)
        )

        MAX_DESCRIPTION_LENGTH = 16 * 1024 * 1024  # 16 MB for MEDIUMTEXT

        
        # Process and insert repositories into the database
        collected_repos = []
        for repo_data in repos_data:
            try:
                full_name = repo_data.get('full_name')
                logger.info(f"Processing repository: {full_name}")
                
                # Check if the repository already exists
                owner, name = full_name.split('/')
                existing_repo = self.db.get_repository_by_owner_and_name(owner, name)
                
                if existing_repo:
                    logger.info(f"Repository {full_name} already exists in the database")
                    collected_repos.append(existing_repo)
                    continue
                
                # Create or retrieve contributor for owner
                owner_type = repo_data.get('owner', {}).get('type')
                owner_id = repo_data.get('owner', {}).get('id')
                owner_login = repo_data.get('owner', {}).get('login')
                
                logger.info(f"Owner info - type: {owner_type}, id: {owner_id}, login: {owner_login}")
                
                owner_obj = self.db.get_contributor_by_login(owner_login)
                if not owner_obj:
                    # Create new contributor
                    owner_data = {
                        'login': owner_login,
                        'id': owner_id,
                        'type': owner_type,
                        'name': owner_login,  # Simplification: Use login as name
                    }
                    owner_obj = self.db.get_or_create_contributor(owner_data)
                
                logger.info(f"Owner object: {owner_obj} with ID: {owner_obj.id if owner_obj else None}")
                
                # If owner is an organization, ensure organization also exists
                org_id = None
                if owner_type == 'Organization':
                    org = self.db.get_organization_by_login(owner_login)
                    if not org:
                        # Create new organization
                        org_data = {
                            'login': owner_login,
                            'id': owner_id,
                            'name': owner_login,
                        }
                        org = self.db.get_or_create_organization(org_data)
                    org_id = org.id
                
#                # Check if the repository description is too long 
                description = repo_data.get('description')
                if description:
                    encoded = description.encode('utf-8')
                    if len(encoded) > MAX_DESCRIPTION_LENGTH:
                        logger.warning(
                            f"Truncating description for repo {repo_data.get('full_name')} "
                            f"from {len(encoded)} bytes to {MAX_DESCRIPTION_LENGTH} bytes"
                        )
                        truncated = encoded[:MAX_DESCRIPTION_LENGTH]
                        description = truncated.decode('utf-8', errors='ignore')
                    #repo.description = description

                # Insert the repository
                repo_data_to_insert = {
                    'id': repo_data.get('id'),
                    'name': repo_data.get('name'),
                    'full_name': full_name,
                    'description': description, #repo_data.get('description'),
                    'language': repo_data.get('language'),
                    'stargazers_count': repo_data.get('stargazers_count'),
                    'watchers_count': repo_data.get('watchers_count'),
                    'forks_count': repo_data.get('forks_count'),
                    'open_issues_count': repo_data.get('open_issues_count'),
                    'created_at': repo_data.get('created_at'),
                    'updated_at': repo_data.get('updated_at'),
                    'owner_id': owner_obj.id if owner_obj else None,
                    'organization_id': org_id,
                }
                
                logger.info(f"Repository data to insert: {repo_data_to_insert}")
                
                repo = self.db.insert_repository(repo_data_to_insert)
                collected_repos.append(repo)
                logger.info(f"Repository {full_name} successfully inserted into the database")
                
            except Exception as e:
                logger.error(f"Error processing repository: {e}")
                import traceback
                traceback.print_exc()
        
        return collected_repos
