"""Tests for repository mapper functionality."""

import unittest
from datetime import datetime
from unittest.mock import Mock, MagicMock, patch
from sqlalchemy.orm import Session

from src.github_database.mapping.repository_mapper import (
    RepositoryMapper,
    EventValidator,
    EventValidationError
)
from src.github_database.config import ETLConfig
from src.github_database.database.database import (
    Repository,
    User,
    Commit,
    PullRequest,
    Issue,
    Event,
    Fork,
    Star,
    Watch
)

class TestEventValidator(unittest.TestCase):
    """Test cases for EventValidator."""
    
    def setUp(self):
        """Set up test environment."""
        self.validator = EventValidator()
        
    def test_validate_valid_event(self):
        """Test validation of well-formed event."""
        event = {
            'id': '12345',
            'type': 'PushEvent',
            'actor': {'id': 1, 'login': 'test'},
            'repo': {'id': 1, 'name': 'test/repo'},
            'created_at': '2024-01-01T00:00:00Z',
            'payload': {
                'ref': 'refs/heads/main',
                'commits': []
            }
        }
        
        # Should not raise exception
        self.validator.validate_event(event)
        
    def test_validate_missing_required_fields(self):
        """Test validation fails with missing fields."""
        event = {
            'id': '12345',
            'type': 'PushEvent'
            # Missing required fields
        }
        
        with self.assertRaises(EventValidationError) as context:
            self.validator.validate_event(event)
            
        self.assertIn("Missing required fields", str(context.exception))
        
    def test_validate_missing_payload_fields(self):
        """Test validation fails with missing payload fields."""
        event = {
            'id': '12345',
            'type': 'PushEvent',
            'actor': {'id': 1},
            'repo': {'id': 1},
            'created_at': '2024-01-01T00:00:00Z',
            'payload': {}  # Missing required payload fields
        }
        
        with self.assertRaises(EventValidationError) as context:
            self.validator.validate_event(event)
            
        self.assertIn("Missing payload fields", str(context.exception))

class TestRepositoryMapper(unittest.TestCase):
    """Test cases for RepositoryMapper."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.session = Mock(spec=Session)
        
        # Create mock ETLConfig
        self.config = Mock(spec=ETLConfig)
        self.config.database = Mock()
        self.config.database.url = '/tmp/test.db'
        self.config.api = Mock()
        self.config.api.token = 'test-token'
        self.config.api.base_url = 'https://api.github.com'
        
        # Mock repository and user
        self.mock_repo = Mock(spec=Repository)
        self.mock_repo.id = 1
        self.mock_user = Mock(spec=User)
        self.mock_user.id = 1
        
        # Configure session mock
        mock_query = Mock()
        mock_query.get.return_value = None  # Simulate no existing records
        self.session.query.return_value = mock_query
        
        # Create mock enricher
        self.enricher = Mock()
        
        # Mock enricher methods
        def enrich_repository(repo_dict):
            return {
                'id': repo_dict['id'],
                'name': repo_dict['name'],
                'description': 'Test repo',
                'language': 'Python',
                'stars': 10,
                'forks': 5
            }
            
        def enrich_user(user_dict):
            return {
                'id': user_dict['id'],
                'login': user_dict.get('login', 'unknown'),
                'name': 'Test User',
                'email': 'test@example.com'
            }
            
        def enrich_commit(commit_dict, repo_name):
            return {
                'sha': commit_dict['sha'],
                'message': commit_dict['message'],
                'author': {
                    'id': 1,
                    'login': 'test',
                    'name': commit_dict['author']['name'],
                    'email': commit_dict['author']['email']
                }
            }
            
        def enrich_issue(issue_dict, repo_name):
            return {
                'id': issue_dict['id'],
                'number': issue_dict['number'],
                'title': issue_dict['title'],
                'body': issue_dict['body'],
                'state': issue_dict['state'],
                'user': {
                    'id': issue_dict['user']['id'],
                    'login': 'test'
                },
                'created_at': issue_dict['created_at'],
                'updated_at': issue_dict['updated_at']
            }
            
        def enrich_pull_request(pr_dict, repo_name):
            return {
                'id': pr_dict['id'],
                'number': pr_dict['number'],
                'title': pr_dict['title'],
                'body': pr_dict['body'],
                'state': pr_dict['state'],
                'user': {
                    'id': pr_dict['user']['id'],
                    'login': 'test'
                },
                'created_at': pr_dict['created_at'],
                'updated_at': pr_dict['updated_at'],
                'base': pr_dict['base'],
                'head': pr_dict['head']
            }
            
        self.enricher.enrich_repository.side_effect = enrich_repository
        self.enricher.enrich_user.side_effect = enrich_user
        self.enricher.enrich_commit.side_effect = enrich_commit
        self.enricher.enrich_issue.side_effect = enrich_issue
        self.enricher.enrich_pull_request.side_effect = enrich_pull_request
        
        self.mapper = RepositoryMapper(self.session, self.config, self.enricher)
        
    def test_map_push_event(self):
        """Test mapping of PushEvent."""
        event = {
            'id': '12345',
            'type': 'PushEvent',
            'actor': {'id': 1, 'login': 'test'},
            'repo': {'id': 1, 'name': 'test/repo'},
            'created_at': '2024-01-01T00:00:00Z',
            'payload': {
                'ref': 'refs/heads/main',
                'commits': [{
                    'sha': 'abc123',
                    'message': 'Test commit',
                    'author': {
                        'name': 'Test Author',
                        'email': 'test@example.com'
                    },
                    'timestamp': '2024-01-01T00:00:00Z'
                }]
            }
        }
        
        # Map event
        event_obj, commits = self.mapper.map_pushevent(event)
        
        self.assertEqual(event_obj.type, 'PushEvent')
        self.assertEqual(event_obj.actor_id, 1)
        self.assertEqual(event_obj.repo_id, 1)
        
        self.assertEqual(len(commits), 1)
        commit = commits[0]
        self.assertEqual(commit.sha, 'abc123')
        self.assertEqual(commit.message, 'Test commit')
        self.assertEqual(commit.repository_id, 1)
        
    def test_map_pull_request_event(self):
        """Test mapping of PullRequestEvent."""
        event = {
            'id': '12345',
            'type': 'PullRequestEvent',
            'actor': {'id': 1, 'login': 'test'},
            'repo': {'id': 1, 'name': 'test/repo'},
            'created_at': '2024-01-01T00:00:00Z',
            'payload': {
                'action': 'opened',
                'pull_request': {
                    'id': 1,
                    'number': 1,
                    'state': 'open',
                    'title': 'Test PR',
                    'body': 'Test body',
                    'user': {'id': 1},
                    'base': {'ref': 'main'},
                    'head': {'ref': 'feature'},
                    'created_at': '2024-01-01T00:00:00Z',
                    'updated_at': '2024-01-01T00:00:00Z'
                }
            }
        }
        
        # Map event
        event_obj, pull_request = self.mapper.map_pullrequestevent(event)
        
        self.assertEqual(event_obj.type, 'PullRequestEvent')
        self.assertEqual(pull_request.title, 'Test PR')
        self.assertEqual(pull_request.state, 'open')
        self.assertEqual(pull_request.base_ref, 'main')
        self.assertEqual(pull_request.head_ref, 'feature')
        
    def test_map_issues_event(self):
        """Test mapping of IssuesEvent."""
        event = {
            'id': '12345',
            'type': 'IssuesEvent',
            'actor': {'id': 1, 'login': 'test'},
            'repo': {'id': 1, 'name': 'test/repo'},
            'created_at': '2024-01-01T00:00:00Z',
            'payload': {
                'action': 'opened',
                'issue': {
                    'id': 1,
                    'number': 1,
                    'state': 'open',
                    'title': 'Test Issue',
                    'body': 'Test body',
                    'user': {'id': 1},
                    'created_at': '2024-01-01T00:00:00Z',
                    'updated_at': '2024-01-01T00:00:00Z'
                }
            }
        }
        
        # Map event
        event_obj, issue = self.mapper.map_issuesevent(event)
        
        self.assertEqual(event_obj.type, 'IssuesEvent')
        self.assertEqual(issue.title, 'Test Issue')
        self.assertEqual(issue.state, 'open')
        
    def test_map_fork_event(self):
        """Test mapping of ForkEvent."""
        event = {
            'id': '12345',
            'type': 'ForkEvent',
            'actor': {'id': 1, 'login': 'test'},
            'repo': {'id': 1, 'name': 'test/repo'},
            'created_at': '2024-01-01T00:00:00Z',
            'payload': {
                'forkee': {
                    'id': 2,
                    'name': 'test/repo-fork'
                }
            }
        }
        
        # Mock forked repository
        mock_fork_repo = Mock(spec=Repository)
        mock_fork_repo.id = 2
        self.session.query().get.side_effect = [self.mock_repo, self.mock_user, mock_fork_repo]
        
        # Map event
        event_obj, fork = self.mapper.map_forkevent(event)
        
        self.assertEqual(event_obj.type, 'ForkEvent')
        self.assertEqual(fork.parent_id, 1)
        self.assertEqual(fork.repository_id, 2)
        
    def test_map_star_event(self):
        """Test mapping of StarEvent."""
        event = {
            'id': '12345',
            'type': 'StarEvent',
            'actor': {'id': 1, 'login': 'test'},
            'repo': {'id': 1, 'name': 'test/repo'},
            'created_at': '2024-01-01T00:00:00Z',
            'payload': {
                'action': 'started'
            }
        }
        
        # Map event
        event_obj, star = self.mapper.map_starevent(event)
        
        self.assertEqual(event_obj.type, 'StarEvent')
        self.assertEqual(star.user_id, 1)
        self.assertEqual(star.repository_id, 1)
        
    def test_map_watch_event(self):
        """Test mapping of WatchEvent."""
        event = {
            'id': '12345',
            'type': 'WatchEvent',
            'actor': {'id': 1, 'login': 'test'},
            'repo': {'id': 1, 'name': 'test/repo'},
            'created_at': '2024-01-01T00:00:00Z',
            'payload': {
                'action': 'started'
            }
        }
        
        # Map event
        event_obj, watch = self.mapper.map_watchevent(event)
        
        self.assertEqual(event_obj.type, 'WatchEvent')
        self.assertEqual(watch.user_id, 1)
        self.assertEqual(watch.repository_id, 1)
        
    def test_extract_timestamp(self):
        """Test timestamp extraction."""
        event = {
            'created_at': '2024-01-01T00:00:00Z'
        }
        
        timestamp = self.mapper._extract_timestamp(event, 'created_at')
        self.assertEqual(
            timestamp.isoformat(),
            '2024-01-01T00:00:00+00:00'
        )
        
    def test_extract_repository(self):
        """Test repository extraction."""
        repo_dict = {
            'id': 1,
            'name': 'test/repo'
        }
        
        repo = self.mapper._extract_repository(repo_dict)
        self.assertEqual(repo.id, 1)
        
    def test_extract_user(self):
        """Test user extraction."""
        user_dict = {
            'id': 1,
            'login': 'test',
            'name': 'Test User'
        }
        
        user = self.mapper._extract_user(user_dict)
        self.assertEqual(user.id, 1)
        
if __name__ == '__main__':
    unittest.main()
