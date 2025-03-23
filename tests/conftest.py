"""Test configuration and fixtures."""

import os
import pytest
from pathlib import Path

@pytest.fixture(autouse=True)
def setup_test_env():
    """Set up test environment variables."""
    # Save original environment
    original_env = dict(os.environ)
    
    # Set test environment variables
    os.environ.update({
        'GITHUB_TOKEN': 'test_token',
        'GITHUB_API_URL': 'https://api.github.com',
        'GITHUB_RATE_LIMIT_DELAY': '0.1',
        'GITHUB_PARALLEL_REQUESTS': '2',
        'GITHUB_RETRY_COUNT': '3',
        'GITHUB_RETRY_DELAY': '1.0',
        'BIGQUERY_PROJECT_ID': 'test-project',
        'BIGQUERY_DATASET_ID': 'github_archive',
        'BIGQUERY_TABLE_ID': 'events',
        'BIGQUERY_MAX_QUERY_DAYS': '30',
        'ETL_CACHE_DIR': '/tmp/github-etl-test',
        'DATABASE_URL': 'sqlite:///:memory:',
        'ETL_BATCH_SIZE': '1000',
        'MIN_STARS': '50',
        'MIN_FORKS': '10',
        'MIN_COMMITS_LAST_YEAR': '100'
    })
    
    yield
    
    # Restore original environment
    os.environ.clear()
    os.environ.update(original_env)
