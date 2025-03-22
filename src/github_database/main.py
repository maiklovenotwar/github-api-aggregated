import os
import time
from datetime import datetime, timedelta
import requests
from typing import Optional, Dict, List, Tuple
from sqlalchemy import inspect

# dotenv for environment variables
from dotenv import load_dotenv

# Own modules from github_database package
from .api.github_api import get_repositories_since
from .database.database import (
    init_db,
    get_session,
    create_repository_from_api,
    Repository
)
from .config.bigquery_config import BigQueryConfig
from .etl_orchestrator import ETLOrchestrator

# Load environment variables
load_dotenv()
GITHUB_TOKEN = os.getenv('GITHUB_API_TOKEN')
headers = {
    'Authorization': f'token {GITHUB_TOKEN}',
    'Accept': 'application/vnd.github.v3+json'
}

class RateLimitManager:
    def __init__(self, threshold: int = 5):  # Lower threshold for Core API
        self.threshold = threshold
        self.remaining = None
        self.reset_time = None
        self._last_check = None
        self._check_interval = timedelta(minutes=1)  # Check every 1 minute
        
    def should_check_rate_limit(self) -> bool:
        """Determine if rate limit should be checked"""
        if self._last_check is None:
            return True
        if self.remaining is not None and self.remaining < self.threshold:
            return True
        return datetime.now() - self._last_check > self._check_interval
        
    def check_rate_limit(self) -> None:
        """Check Core API rate limit only when necessary"""
        if not self.should_check_rate_limit():
            return
            
        url = "https://api.github.com/rate_limit"
        response = requests.get(url, headers=headers)
        self._last_check = datetime.now()
        
        if response.status_code == 200:
            data = response.json()
            core = data.get("resources", {}).get("core", {})
            self.remaining = core.get("remaining", 0)
            reset = core.get("reset", 0)
            self.reset_time = datetime.fromtimestamp(reset)
            limit = core.get("limit", 5000)
            
            print(f"Core-API Rate-Limit: {self.remaining}/{limit} remaining (Reset: {self.reset_time})")
            
            if self.remaining < self.threshold:
                sleep_seconds = (self.reset_time - datetime.now()).total_seconds() + 5
                if sleep_seconds > 0:
                    print(f"Core-API Rate-Limit low. Waiting {int(sleep_seconds)} seconds...")
                    time.sleep(sleep_seconds)
        else:
            print(f"Error fetching rate limit: HTTP {response.status_code}")

def process_repositories(repositories: List[Dict], session) -> Tuple[int, int]:
    """
    Process a list of repositories and save them to the database.
    Uses SQLAlchemy bulk operations for better performance.
    
    Returns:
        Tuple of (number of new repos, number of updated repos)
    """
    # Get all columns of the Repository model
    columns = [c.key for c in inspect(Repository).mapper.column_attrs]
    
    # Collect new and to-be-updated repositories
    new_repos_dicts = []
    update_dicts = []
    existing_repo_ids = set(id_tuple[0] for id_tuple in 
                          session.query(Repository.repo_id)
                          .filter(Repository.repo_id.in_([r['id'] for r in repositories]))
                          .all())
    
    for repo_data in repositories:
        try:
            # Create Repository object
            repo_obj = create_repository_from_api(repo_data)
            
            # Convert to dictionary for bulk operations
            repo_dict = {attr: getattr(repo_obj, attr) for attr in columns}
            
            if repo_obj.repo_id in existing_repo_ids:
                update_dicts.append(repo_dict)
            else:
                new_repos_dicts.append(repo_dict)
                
        except Exception as e:
            print(f"Error processing repository {repo_data.get('full_name')}: {e}")
            continue
            
    # Bulk insert new repositories
    if new_repos_dicts:
        session.bulk_insert_mappings(Repository, new_repos_dicts)
        
    # Bulk update existing repositories
    if update_dicts:
        session.bulk_update_mappings(Repository, update_dicts)
        
    session.commit()
    
    return len(new_repos_dicts), len(update_dicts)

def main():
    """Main entry point for GitHub data collection."""
    # Initialize database
    init_db()
    
    # Initialize rate limit manager
    rate_limit = RateLimitManager()
    
    # Create configurations
    bigquery_config = BigQueryConfig.from_env()
    
    # Initialize ETL orchestrator
    orchestrator = ETLOrchestrator(
        config={
            'min_stars': 50,
            'min_forks': 10,
            'min_commits': 100,
            'batch_size': 1000,
            'cache_dir': 'cache'
        },
        bigquery_config=bigquery_config
    )
    
    # Process repositories and events
    try:
        # Get repositories from GitHub API
        session = get_session()
        
        # Get repositories updated in the last week
        since = datetime.now() - timedelta(days=7)
        repositories = get_repositories_since(since, headers)
        
        # Process repositories
        new_count, updated_count = process_repositories(repositories, session)
        print(f"Processed {new_count} new and {updated_count} updated repositories")
        
        # Get historical data from BigQuery
        start_date = datetime(2014, 1, 1)  # GitHub Archive data starts from 2014
        end_date = datetime.now()
        
        # Process historical events for repositories
        orchestrator.process_repositories(
            start_date=start_date,
            end_date=end_date
        )
        
    except Exception as e:
        print(f"Error in main process: {e}")
        raise
        
    finally:
        session.close()

if __name__ == "__main__":
    main()