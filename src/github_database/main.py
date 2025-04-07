"""Main entry point for GitHub data collection and aggregation.

This module serves as the entry point for the GitHub data collection and analysis system
that focuses on geographical patterns in open-source contributions.
"""

import logging
import os
import time
import argparse
import ast
from datetime import datetime, timezone, timedelta
from pathlib import Path

from .config import ETLConfig, GitHubConfig
from .database.database import init_db
from .etl_orchestrator import ETLOrchestrator
from .aggregation import DataAggregator
from .optimized_collector import collect_repositories_parallel
from .collection_strategies import create_collection_strategy, DEFAULT_STAR_RANGES
from .api.token_pool import TokenPool

# Configure logging
logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Main entry point for GitHub data collection, geographic enrichment, and analysis."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="GitHub Global Contribution Analysis - Collect, enrich, and analyze GitHub data with geographic focus"
    )
    
    # Main operation mode
    parser.add_argument(
        "--mode", 
        choices=["collect", "enrich", "aggregate", "export"], 
        default="collect",
        help="Operation mode: collect data, enrich with geographic info, aggregate analytics, or export data"
    )
    
    # Collection parameters
    collection_group = parser.add_argument_group('Data Collection Options')
    collection_group.add_argument(
        "--collection-strategy", 
        choices=["star-ranges", "time-period"], 
        default="star-ranges",
        help="Strategy for collecting repositories: star-ranges (by popularity) or time-period (by creation date)"
    )
    collection_group.add_argument(
        "--star-ranges", 
        type=str, 
        default=None,
        help="Custom star ranges in format [(min1, max1), (min2, max2), ...]. If not provided, DEFAULT_STAR_RANGES is used"
    )
    collection_group.add_argument(
        "--start-year", 
        type=int, 
        default=2008,
        help="Start year for time-period collection (default: 2008, GitHub's founding year)"
    )
    collection_group.add_argument(
        "--end-year", 
        type=int, 
        default=datetime.now().year,
        help=f"End year for time-period collection (default: current year)"
    )
    collection_group.add_argument(
        "--min-stars", 
        type=int, 
        default=10,
        help="Minimum stars for repositories (default: 10)"
    )
    collection_group.add_argument(
        "--min-forks", 
        type=int, 
        default=0,
        help="Minimum forks for repositories (default: 0)"
    )
    collection_group.add_argument(
        "--limit", 
        type=int, 
        default=1000,
        help="Limit number of repositories to process (default: 1000)"
    )
    
    # Parallelization and optimization
    parallel_group = parser.add_argument_group('Parallel Processing Options')
    parallel_group.add_argument(
        "--parallel", 
        action="store_true",
        help="Use parallel processing for data collection"
    )
    parallel_group.add_argument(
        "--workers", 
        type=int, 
        default=10,
        help="Number of parallel workers for data collection (default: 10)"
    )
    parallel_group.add_argument(
        "--batch-size", 
        type=int, 
        default=100,
        help="Batch size for parallel processing (default: 100)"
    )
    parallel_group.add_argument(
        "--tokens", 
        nargs="+", 
        default=None,
        help="List of GitHub API tokens to use (default: use GITHUB_API_TOKEN from environment)"
    )
    
    # Geographical enrichment options
    geo_group = parser.add_argument_group('Geographical Enrichment Options')
    geo_group.add_argument(
        "--geocode-locations", 
        action="store_true",
        help="Geocode user and organization locations to extract country and region information"
    )
    geo_group.add_argument(
        "--update-existing", 
        action="store_true",
        help="Update geographical data for existing repositories, contributors, and organizations"
    )
    
    # Export options
    export_group = parser.add_argument_group('Export Options')
    export_group.add_argument(
        "--output-dir", 
        default="./data/exports",
        help="Directory for exporting data (default: ./data/exports)"
    )
    
    # System options
    system_group = parser.add_argument_group('System Options')
    system_group.add_argument(
        "--reset-db", 
        action="store_true",
        help="Reset database before starting (default: False)"
    )
    
    args = parser.parse_args()
    
    # Log configuration
    logger.info(f"Mode: {args.mode}, Collection strategy: {args.collection_strategy}")
    logger.info(f"Repository filters: min_stars={args.min_stars}, min_forks={args.min_forks}, limit={args.limit}")
    
    if args.parallel:
        logger.info(f"Parallel processing enabled with {args.workers} workers and batch size {args.batch_size}")
        if args.tokens:
            token_count = len(args.tokens)
            logger.info(f"Using {token_count} GitHub API tokens")
    
    try:
        # Initialize configuration
        config = ETLConfig(
            database_url=os.getenv("DATABASE_URL", "sqlite:///github_data.db"),
            github=GitHubConfig(access_token=os.getenv("GITHUB_API_TOKEN", ""))
        )
        
        # Update config with command line parameters
        config.min_stars = args.min_stars
        config.min_forks = args.min_forks
        config.limit = args.limit
        
        # Initialize database
        logger.info("Initializing database...")
        try:
            Session = init_db(config.database_url, args.reset_db)
            session = Session()
            logger.info("Database initialized and session created successfully")
        except Exception as e:
            logger.error(f"Error initializing database: {e}")
            return
        
        # Create token pool if multiple tokens are provided
        token_pool = None
        if args.tokens and len(args.tokens) > 1:
            token_pool = TokenPool(args.tokens)
            logger.info(f"Created token pool with {len(args.tokens)} tokens")
        
        # Execute based on mode
        if args.mode == "collect":
            # Data collection via GitHub API
            if args.parallel:
                # Parallel processing
                collect_data_parallel(
                    config, 
                    Session, 
                    args.workers, 
                    args.batch_size, 
                    args.tokens, 
                    args.collection_strategy,
                    args.star_ranges,
                    args.start_year,
                    args.end_year
                )
            else:
                # Sequential processing
                collect_data(
                    config, 
                    session, 
                    args.collection_strategy,
                    args.star_ranges,
                    args.start_year, 
                    args.end_year,
                    token_pool
                )
        elif args.mode == "enrich":
            # Geographical data enrichment
            enrich_geographical_data(config, session, args.update_existing)
        elif args.mode == "aggregate":
            # Data aggregation from GitHub data
            aggregate_data(config, session, args.start_year, args.end_year)
        elif args.mode == "export":
            # Data export
            export_data(config, session, args.output_dir)
        
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        if 'session' in locals():
            session.close()
            logger.info("Database session closed")

def collect_data_parallel(
    config, 
    session_factory, 
    max_workers=10, 
    batch_size=100, 
    tokens=None, 
    collection_strategy="star-ranges",
    star_ranges=None,
    start_year=None,
    end_year=None
):
    """Collect data from GitHub API using parallel processing.
    
    Args:
        config: ETL configuration
        session_factory: Factory function for database sessions
        max_workers: Maximum number of parallel workers
        batch_size: Batch size for processing repositories
        tokens: List of GitHub API tokens to use
        collection_strategy: Strategy for collecting repositories
        star_ranges: Custom star ranges for collection
        start_year: Start year for time-period collection
        end_year: End year for time-period collection
    """
    try:
        logger.info(f"Starting parallel data collection with {max_workers} workers and batch size {batch_size}")
        logger.info(f"Using collection strategy: {collection_strategy}")
        
        # Parse custom star ranges if provided
        custom_ranges = None
        if star_ranges:
            try:
                custom_ranges = ast.literal_eval(star_ranges)
                logger.info(f"Using custom star ranges: {custom_ranges}")
            except (SyntaxError, ValueError) as e:
                logger.error(f"Failed to parse custom star ranges: {e}. Using default ranges.")
        
        # Use the optimized parallel collector
        results = collect_repositories_parallel(
            config=config,
            session_factory=session_factory,
            max_workers=max_workers,
            batch_size=batch_size,
            tokens=tokens,
            collection_strategy=collection_strategy,
            star_ranges=custom_ranges,
            start_year=start_year,
            end_year=end_year
        )
        
        # Summary
        success_count = sum(1 for _, success in results if success)
        logger.info(f"Parallel data collection completed: {success_count}/{len(results)} repositories successfully processed")
        
    except Exception as e:
        logger.error(f"Error in parallel data collection: {e}")
        import traceback
        logger.error(traceback.format_exc())

def collect_data(
    config, 
    session, 
    collection_strategy="star-ranges",
    star_ranges=None,
    start_year=None, 
    end_year=None,
    token_pool=None
):
    """Collect data from GitHub API using the specified collection strategy.
    
    Args:
        config: ETL configuration
        session: Database session
        collection_strategy: Strategy for collecting repositories
        star_ranges: Custom star ranges for star-ranges strategy
        start_year: Start year for time-period collection
        end_year: End year for time-period collection
        token_pool: Optional token pool for GitHub API
    """
    try:
        # Create ETL orchestrator
        session_factory = lambda: session
        orchestrator = ETLOrchestrator(config, session_factory, token_pool)
        
        logger.info(f"Starting data collection using '{collection_strategy}' strategy")
        
        if collection_strategy == "star-ranges":
            # Parse custom star ranges if provided
            custom_ranges = None
            if star_ranges:
                try:
                    custom_ranges = ast.literal_eval(star_ranges)
                    logger.info(f"Using custom star ranges: {custom_ranges}")
                except (SyntaxError, ValueError) as e:
                    logger.error(f"Failed to parse custom star ranges: {e}. Using default ranges.")
            
            # Use default ranges if no custom ranges provided or parsing failed
            star_ranges_list = custom_ranges or DEFAULT_STAR_RANGES
            
            # Create star range collection strategy
            github_api = orchestrator.github_api
            strategy = create_collection_strategy("star_range", github_api, config.min_stars)
            
            # Override the default ranges with our parsed ranges
            strategy.star_ranges = star_ranges_list
            
            logger.info(f"Using star ranges collection strategy with {len(star_ranges_list)} ranges")
            for i, (min_stars, max_stars) in enumerate(star_ranges_list):
                max_str = str(max_stars) if max_stars is not None else "∞"
                logger.info(f"Range {i+1}: {min_stars} to {max_str} stars")
            
            # Collect repositories
            total_collected = 0
            for range_tuple in star_ranges_list:
                min_stars, max_stars = range_tuple
                max_str = str(max_stars) if max_stars is not None else "∞"
                logger.info(f"Collecting repositories with {min_stars} to {max_str} stars...")
                
                start_time = time.time()
                collected = strategy.get_repositories(
                    session=session,
                    limit=config.limit,
                    min_forks=config.min_forks,
                    current_range=range_tuple
                )
                
                elapsed = time.time() - start_time
                total_collected += len(collected)
                
                logger.info(f"Collected {len(collected)} repositories with {min_stars} to {max_str} stars in {elapsed:.2f} seconds")
                logger.info(f"Total collected so far: {total_collected}")
                
                # Process collected repositories to extract geographical data
                for repo in collected:
                    try:
                        full_name = repo["full_name"]
                        logger.info(f"Processing repository {full_name}")
                        repository = orchestrator.process_repository(full_name, session)
                        if repository:
                            logger.info(f"Successfully processed repository {full_name}")
                        else:
                            logger.warning(f"Failed to process repository {full_name}")
                    except Exception as e:
                        logger.error(f"Error processing repository {repo['full_name']}: {e}")
                
                # Break if we've collected enough repositories
                if total_collected >= config.limit:
                    logger.info(f"Reached collection limit of {config.limit} repositories")
                    break
            
            logger.info(f"Total repositories collected: {total_collected}")
            
        elif collection_strategy == "time-period":
            # Set default years if not provided
            start_year = start_year or 2008  # GitHub founding year
            end_year = end_year or datetime.now().year
            
            logger.info(f"Using time period collection strategy from {start_year} to {end_year}")
            
            # Create time period collection strategy
            github_api = orchestrator.github_api
            strategy = create_collection_strategy("time_period", github_api, config.min_stars)
            
            # Collect repositories
            collected = strategy.get_repositories(
                session=session,
                limit=config.limit,
                min_forks=config.min_forks,
                start_year=start_year,
                end_year=end_year
            )
            
            logger.info(f"Collected {len(collected)} repositories from time period {start_year}-{end_year}")
            
            # Process collected repositories to extract geographical data
            for repo in collected:
                try:
                    full_name = repo["full_name"]
                    logger.info(f"Processing repository {full_name}")
                    repository = orchestrator.process_repository(full_name, session)
                    if repository:
                        logger.info(f"Successfully processed repository {full_name}")
                    else:
                        logger.warning(f"Failed to process repository {full_name}")
                except Exception as e:
                    logger.error(f"Error processing repository {repo['full_name']}: {e}")
            
        else:
            logger.error(f"Unknown collection strategy: {collection_strategy}")
        
    except Exception as e:
        logger.error(f"Error in data collection: {e}")
        import traceback
        logger.error(traceback.format_exc())

def enrich_geographical_data(config, session, update_existing=False):
    """Enrich database with geographical information from contributor and organization profiles.
    
    Args:
        config: ETL configuration
        session: Database session
        update_existing: Whether to update existing geographical data
    """
    try:
        logger.info("Starting geographical data enrichment")
        
        # Create ETL orchestrator
        session_factory = lambda: session
        orchestrator = ETLOrchestrator(config, session_factory)
        
        # Get contributors and organizations with location data but missing country code
        if update_existing:
            logger.info("Processing all contributors and organizations with location data")
            contributors = session.query(Contributor).filter(Contributor.location.isnot(None)).all()
            organizations = session.query(Organization).filter(Organization.location.isnot(None)).all()
        else:
            logger.info("Processing only contributors and organizations with location data but missing country code")
            contributors = session.query(Contributor).filter(
                Contributor.location.isnot(None),
                Contributor.country_code.is_(None)
            ).all()
            organizations = session.query(Organization).filter(
                Organization.location.isnot(None),
                Organization.country_code.is_(None)
            ).all()
        
        # Process contributors
        logger.info(f"Processing {len(contributors)} contributors for geographical enrichment")
        processed_contributors = 0
        for contributor in contributors:
            try:
                location_data = orchestrator.geocode_location(contributor.location)
                if location_data:
                    contributor.country_code = location_data.get('country_code')
                    contributor.region = location_data.get('region')
                    processed_contributors += 1
                    if processed_contributors % 100 == 0:
                        logger.info(f"Processed {processed_contributors}/{len(contributors)} contributors")
                        session.commit()
            except Exception as e:
                logger.error(f"Error geocoding contributor location '{contributor.location}': {e}")
        
        session.commit()
        logger.info(f"Completed geographical enrichment for {processed_contributors}/{len(contributors)} contributors")
        
        # Process organizations
        logger.info(f"Processing {len(organizations)} organizations for geographical enrichment")
        processed_orgs = 0
        for org in organizations:
            try:
                location_data = orchestrator.geocode_location(org.location)
                if location_data:
                    org.country_code = location_data.get('country_code')
                    org.region = location_data.get('region')
                    processed_orgs += 1
                    if processed_orgs % 100 == 0:
                        logger.info(f"Processed {processed_orgs}/{len(organizations)} organizations")
                        session.commit()
            except Exception as e:
                logger.error(f"Error geocoding organization location '{org.location}': {e}")
        
        session.commit()
        logger.info(f"Completed geographical enrichment for {processed_orgs}/{len(organizations)} organizations")
        
    except Exception as e:
        logger.error(f"Error in geographical data enrichment: {e}")
        import traceback
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    main()