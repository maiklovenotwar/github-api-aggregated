"""Tests for BigQuery API client."""

import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch

from src.github_database.api.bigquery_api import BigQueryClient
from src.github_database.config import BigQueryConfig

@pytest.fixture
def bigquery_client():
    """Create test BigQuery client."""
    config = BigQueryConfig(
        project_id="test-project",
        dataset_id="github_archive",
        table_id="day"
    )
    return BigQueryClient(config)

def test_get_repository_metrics(bigquery_client):
    """Test getting repository metrics."""
    # Mock query result
    mock_result = [{
        'stars': 100,
        'contributors': 10,
        'commits': 500
    }]
    
    bigquery_client._execute_query = MagicMock(return_value=mock_result)
    
    # Test metrics retrieval
    metrics = bigquery_client.get_repository_metrics(
        full_name='test/repo',
        since=datetime.now(timezone.utc) - timedelta(days=30)
    )
    
    assert metrics['stars'] == 100
    assert metrics['contributors'] == 10
    assert metrics['commits'] == 500

def test_get_repository_events(bigquery_client):
    """Test getting repository events."""
    # Mock query result
    mock_result = [{
        'id': '1',
        'type': 'PushEvent',
        'created_at': datetime.now(timezone.utc),
        'actor': {'id': 1, 'login': 'user1'},
        'repo': {'id': 1, 'name': 'test/repo'},
        'payload': {'ref': 'refs/heads/main'}
    }]
    
    bigquery_client._execute_query = MagicMock(return_value=mock_result)
    
    # Test event retrieval
    events = bigquery_client.get_repository_events(
        full_name='test/repo',
        event_types=['PushEvent'],
        since=datetime.now(timezone.utc) - timedelta(days=30)
    )
    
    assert len(events) == 1
    assert events[0]['type'] == 'PushEvent'
    assert events[0]['actor']['login'] == 'user1'

def test_empty_results(bigquery_client):
    """Test handling of empty results."""
    bigquery_client._execute_query = MagicMock(return_value=[])
    
    # Test metrics
    metrics = bigquery_client.get_repository_metrics(
        full_name='test/repo',
        since=datetime.now(timezone.utc) - timedelta(days=30)
    )
    assert metrics == {'stars': 0, 'contributors': 0, 'commits': 0}
    
    # Test events
    events = bigquery_client.get_repository_events(
        full_name='test/repo',
        since=datetime.now(timezone.utc) - timedelta(days=1)
    )
    assert events == []
