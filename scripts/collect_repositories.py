#!/usr/bin/env python3
"""
Script to collect GitHub repositories.
"""
import os
import sys
import logging
import argparse
from datetime import datetime, timedelta
from dateutil import tz

# Add the src directory to the Python path
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__)), "src"))

from github_database.api import SimpleGitHubClient
from github_database.database import GitHubDatabase
from github_database.repository_collector import RepositoryCollector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("repository_collection.log")
    ]
)
logger = logging.getLogger(__name__)


def setup_api_client():
    """Set up the GitHub API client."""
    # Load environment variables from .env file if available
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        logger.warning("python-dotenv not installed, skipping .env loading")
    
    # Get GitHub API token from environment variable
    github_token = os.environ.get("GITHUB_API_TOKEN")
    if not github_token:
        logger.warning("GitHub API token not found in environment variables")
        github_token = input("Please enter your GitHub API token: ").strip()
        if not github_token:
            raise ValueError("GitHub API token is required")
    
    # Create and return the API client
    return SimpleGitHubClient(github_token)


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Collect GitHub repositories")
    
    # Time range options
    time_group = parser.add_argument_group("Time Range Options")
    time_group.add_argument("--time-range", choices=["week", "month", "year", "custom"], 
                           help="Predefined time range for repository collection")
    time_group.add_argument("--start-date", help="Start date for custom time range (YYYY-MM-DD)")
    time_group.add_argument("--end-date", help="End date for custom time range (YYYY-MM-DD)")
    
    # Collection options
    collection_group = parser.add_argument_group("Collection Options")
    collection_group.add_argument("--limit", type=int, help="Maximum number of repositories to collect")
    collection_group.add_argument("--all", action="store_true", help="Collect all available repositories")
    collection_group.add_argument("--min-stars", type=int, default=100, 
                                help="Minimum number of stars for repositories (default: 100)")
    
    # Database options
    db_group = parser.add_argument_group("Database Options")
    db_group.add_argument("--db-path", help="Path to the SQLite database file")
    
    # Other options
    parser.add_argument("--non-interactive", action="store_true", 
                       help="Run in non-interactive mode (requires time range and limit options)")
    parser.add_argument("--stats", action="store_true", 
                       help="Show database statistics and exit")
    
    return parser.parse_args()


def show_database_stats(db):
    """Show statistics about the database."""
    print("\n=== Database Statistics ===")
    print(f"Repositories: {db.get_repository_count()}")
    print(f"Contributors: {db.get_contributor_count()}")
    print(f"Organizations: {db.get_organization_count()}")
    
    # Show language statistics if available
    try:
        languages = db.get_language_statistics()
        if languages:
            print("\nTop Languages:")
            for lang, count in languages[:10]:
                print(f"  {lang}: {count} repositories")
    except Exception as e:
        logger.debug(f"Could not get language statistics: {e}")
    
    # Show creation date range
    try:
        date_range = db.get_repository_date_range()
        if date_range:
            print(f"\nRepository date range: {date_range[0]} to {date_range[1]}")
    except Exception as e:
        logger.debug(f"Could not get repository date range: {e}")
        
    # Show location and country code statistics for contributors
    try:
        contributor_stats = db.get_contributor_location_stats()
        if contributor_stats:
            print("\nContributor Location Statistics:")
            print(f"  Total contributors: {contributor_stats['total']}")
            print(f"  Contributors with location: {contributor_stats['with_location']} ({contributor_stats['location_percentage']:.1f}%)")
            print(f"  Contributors with country code: {contributor_stats['with_country_code']} ({contributor_stats['country_code_percentage']:.1f}%)")
            if contributor_stats['with_location'] > 0:
                print(f"  Country code resolution rate: {contributor_stats['country_code_from_location_percentage']:.1f}% of contributors with location")
    except Exception as e:
        logger.debug(f"Could not get contributor location statistics: {e}")
        
    # Show location and country code statistics for organizations
    try:
        org_stats = db.get_organization_location_stats()
        if org_stats:
            print("\nOrganization Location Statistics:")
            print(f"  Total organizations: {org_stats['total']}")
            print(f"  Organizations with location: {org_stats['with_location']} ({org_stats['location_percentage']:.1f}%)")
            print(f"  Organizations with country code: {org_stats['with_country_code']} ({org_stats['country_code_percentage']:.1f}%)")
            if org_stats['with_location'] > 0:
                print(f"  Country code resolution rate: {org_stats['country_code_from_location_percentage']:.1f}% of organizations with location")
    except Exception as e:
        logger.debug(f"Could not get organization location statistics: {e}")


def interactive_mode(args, api_client, db):
    """Run in interactive mode."""
    # Show current stats
    show_database_stats(db)
    
    # Initialize repository collector
    collector = RepositoryCollector(github_client=api_client, db=db)
    
    # Ask for time range if not provided
    if not args.time_range:
        print("\nSelect a time range for repository collection:")
        print("1. Last week")
        print("2. Last month")
        print("3. Last year")
        print("4. Custom time range")
        
        choice = input("Your choice (1-4): ").strip()
        
        if choice == "1":
            args.time_range = "week"
        elif choice == "2":
            args.time_range = "month"
        elif choice == "3":
            args.time_range = "year"
        elif choice == "4":
            args.time_range = "custom"
        else:
            logger.error("Invalid selection")
            return
    
    # Calculate time range
    now = datetime.now(tz.UTC)
    
    if args.time_range == "week":
        # Last week
        end_date = now
        start_date = now - timedelta(days=7)
    elif args.time_range == "month":
        # Last month
        end_date = now
        start_date = now - timedelta(days=30)
    elif args.time_range == "year":
        # Last year
        end_date = now
        start_date = now - timedelta(days=365)
    elif args.time_range == "custom":
        # Custom time range
        if args.start_date and args.end_date:
            try:
                start_date = datetime.strptime(args.start_date, "%Y-%m-%d").replace(tzinfo=tz.UTC)
                end_date = datetime.strptime(args.end_date, "%Y-%m-%d").replace(tzinfo=tz.UTC)
            except ValueError as e:
                logger.error(f"Invalid date format: {e}")
                return
        else:
            try:
                print("Start date (YYYY-MM-DD):")
                start_date_str = input().strip()
                start_date = datetime.strptime(start_date_str, "%Y-%m-%d").replace(tzinfo=tz.UTC)
                
                print("End date (YYYY-MM-DD):")
                end_date_str = input().strip()
                end_date = datetime.strptime(end_date_str, "%Y-%m-%d").replace(tzinfo=tz.UTC)
            except ValueError as e:
                logger.error(f"Invalid date format: {e}")
                return
    else:
        logger.error("Invalid time range")
        return
    
    # Ask for minimum stars
    if not args.min_stars:
        try:
            min_stars_str = input("\nMinimum number of stars (default: 100): ").strip()
            args.min_stars = int(min_stars_str) if min_stars_str else 100
        except ValueError:
            logger.warning("Invalid number, using default value 100")
            args.min_stars = 100
    
    # Ask for limit if not provided
    if not args.limit and not args.all:
        try:
            print("\nMaximum number of repositories to collect:")
            print("Options:")
            print("1. Enter a specific number (e.g., 100, 500, 1000)")
            print("2. Enter 'all' to collect all available repositories")
            print("   Note: This may take a long time and will process repositories in batches")
            limit_str = input("\nYour choice: ").strip().lower()
            
            if limit_str == 'all':
                args.all = True
                args.limit = None
                print("Will collect all available repositories")
            else:
                try:
                    args.limit = int(limit_str)
                    if args.limit <= 0:
                        print("Invalid number, using default value 100")
                        args.limit = 100
                    print(f"Will collect up to {args.limit} repositories")
                except ValueError:
                    print("Invalid input, using default value 100")
                    args.limit = 100
        except Exception as e:
            logger.warning(f"Error setting limit: {e}")
            args.limit = 100
            print(f"Using default value: {args.limit}")
    
    # Set limit based on all flag
    if args.all:
        args.limit = None
    
    # Collect repositories
    print(f"\nCollecting repositories from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}...")
    print(f"Minimum stars: {args.min_stars}")
    if args.limit is None:
        print("This will collect ALL repositories matching the criteria.")
        print("The process will break down the time range into smaller periods to work around GitHub API limitations.")
    else:
        print(f"Will collect up to {args.limit} repositories")
    print("This may take some time depending on the number of repositories.")
    print("Progress will be shown as repositories are collected.\n")
    
    # Start collection with progress tracking
    try:
        import time
        start_time = time.time()
        
        # Update the query to include min_stars
        query_template = f"created:{{}} stars:>={args.min_stars}"
        
        # Collect repositories
        repositories = collector.collect_repositories(
            start_date, 
            end_date, 
            args.limit,
            args.min_stars
        )
        
        # Calculate time taken
        end_time = time.time()
        duration = end_time - start_time
        
        # Print results
        if repositories:
            print(f"\n=== Collected {len(repositories)} Repositories ===")
            print(f"Time taken: {duration:.1f} seconds ({duration/60:.1f} minutes)")
            
            # Show sample of repositories
            max_display = min(10, len(repositories))
            print(f"\nShowing {max_display} of {len(repositories)} repositories:")
            
            for i, repo in enumerate(repositories[:max_display], 1):
                print(f"{i}. {repo.full_name}")
                print(f"   Stars: {repo.stargazers_count}")
                print(f"   Language: {repo.language or 'Unknown'}")
                print(f"   Description: {repo.description or 'No description'}")
                if hasattr(repo, 'created_at') and repo.created_at:
                    created_at = repo.created_at
                    if isinstance(created_at, str):
                        # Format string datetime if needed
                        try:
                            created_at = datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%SZ")
                        except ValueError:
                            pass
                    print(f"   Created at: {created_at}")
                print()
        else:
            print("\nNo repositories found or collected in this time period.")
        
        # Print updated stats
        show_database_stats(db)
        
    except KeyboardInterrupt:
        print("\nCollection interrupted by user.")
        print("Partial results have been saved to the database.")
        show_database_stats(db)
    except Exception as e:
        logger.error(f"Error during collection: {e}")
        import traceback
        traceback.print_exc()


def non_interactive_mode(args, api_client, db):
    """Run in non-interactive mode."""
    # Validate required arguments
    if not args.time_range:
        logger.error("Time range is required in non-interactive mode")
        return
    
    if args.time_range == "custom" and (not args.start_date or not args.end_date):
        logger.error("Start date and end date are required for custom time range")
        return
    
    if not args.limit and not args.all:
        logger.error("Either --limit or --all is required in non-interactive mode")
        return
    
    # Initialize repository collector
    collector = RepositoryCollector(github_client=api_client, db=db)
    
    # Calculate time range
    now = datetime.now(tz.UTC)
    
    if args.time_range == "week":
        # Last week
        end_date = now
        start_date = now - timedelta(days=7)
    elif args.time_range == "month":
        # Last month
        end_date = now
        start_date = now - timedelta(days=30)
    elif args.time_range == "year":
        # Last year
        end_date = now
        start_date = now - timedelta(days=365)
    elif args.time_range == "custom":
        # Custom time range
        try:
            start_date = datetime.strptime(args.start_date, "%Y-%m-%d").replace(tzinfo=tz.UTC)
            end_date = datetime.strptime(args.end_date, "%Y-%m-%d").replace(tzinfo=tz.UTC)
        except ValueError as e:
            logger.error(f"Invalid date format: {e}")
            return
    else:
        logger.error("Invalid time range")
        return
    
    # Set limit based on all flag
    if args.all:
        args.limit = None
    
    # Collect repositories
    logger.info(f"Collecting repositories from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    logger.info(f"Minimum stars: {args.min_stars}")
    if args.limit is None:
        logger.info("Collecting ALL repositories matching the criteria")
    else:
        logger.info(f"Collecting up to {args.limit} repositories")
    
    # Start collection with progress tracking
    try:
        import time
        start_time = time.time()
        
        # Collect repositories
        repositories = collector.collect_repositories(
            start_date, 
            end_date, 
            args.limit,
            args.min_stars
        )
        
        # Calculate time taken
        end_time = time.time()
        duration = end_time - start_time
        
        # Print results
        logger.info(f"Collected {len(repositories)} repositories in {duration:.1f} seconds")
        
    except KeyboardInterrupt:
        logger.info("Collection interrupted by user")
    except Exception as e:
        logger.error(f"Error during collection: {e}")
        import traceback
        traceback.print_exc()


def main():
    """Main function."""
    try:
        # Parse command line arguments
        args = parse_arguments()
        
        # Set up the API client
        logger.info("Setting up API client")
        api_client = setup_api_client()
        
        # Connect to the database
        db_path = args.db_path if args.db_path else os.path.join(os.path.dirname(os.path.dirname(__file__)), "github_data.db")
        db = GitHubDatabase(db_path=db_path)
        logger.info(f"Connected to database: {db_path}")
        
        # Show stats and exit if requested
        if args.stats:
            show_database_stats(db)
            return
        
        # Run in appropriate mode
        if args.non_interactive:
            non_interactive_mode(args, api_client, db)
        else:
            interactive_mode(args, api_client, db)
        
    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if 'db' in locals():
            db.close()
            logger.info("Database connection closed")


if __name__ == "__main__":
    main()
