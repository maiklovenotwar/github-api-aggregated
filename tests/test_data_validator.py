"""Tests for data validation functionality."""

import unittest
from datetime import datetime
from src.github_database.control_database.validate_data import DataValidator

class TestDataValidator(unittest.TestCase):
    """Test cases for DataValidator class."""
    
    def setUp(self):
        """Set up test environment."""
        self.validator = DataValidator()
        
    def test_validate_events(self):
        """Test validation of event list."""
        events = [
            {
                "id": "1",
                "type": "PushEvent",
                "actor": {"id": 1, "login": "user1"},
                "repo": {"id": 1, "name": "repo1"},
                "created_at": "2025-03-20T12:00:00Z",
                "payload": {
                    "commits": [
                        {
                            "sha": "abc123",
                            "message": "test commit",
                            "author": {"name": "User", "email": "user@test.com"}
                        }
                    ]
                }
            },
            {
                "id": "2",  # Invalid event (missing required fields)
                "type": "PushEvent"
            }
        ]
        
        valid_events = self.validator.validate_events(events)
        self.assertEqual(len(valid_events), 1)
        self.assertEqual(valid_events[0]["id"], "1")
        
    def test_validate_basic_structure(self):
        """Test basic event structure validation."""
        # Valid event
        valid_event = {
            "id": "1",
            "type": "PushEvent",
            "actor": {"id": 1, "login": "user1"},
            "repo": {"id": 1, "name": "repo1"},
            "created_at": "2025-03-20T12:00:00Z"
        }
        
        self.assertTrue(self.validator._validate_basic_structure(valid_event))
        
        # Invalid event (missing fields)
        invalid_event = {
            "id": "1",
            "type": "PushEvent"
        }
        
        self.assertFalse(self.validator._validate_basic_structure(invalid_event))
        
    def test_validate_actor(self):
        """Test actor validation."""
        # Valid actor
        valid_actor = {
            "id": 1,
            "login": "user1"
        }
        
        self.assertTrue(self.validator._validate_actor(valid_actor))
        
        # Invalid actor
        invalid_actor = {
            "id": 1
        }
        
        self.assertFalse(self.validator._validate_actor(invalid_actor))
        
    def test_validate_repo(self):
        """Test repository validation."""
        # Valid repo
        valid_repo = {
            "id": 1,
            "name": "repo1"
        }
        
        self.assertTrue(self.validator._validate_repo(valid_repo))
        
        # Invalid repo
        invalid_repo = {
            "id": 1
        }
        
        self.assertFalse(self.validator._validate_repo(invalid_repo))
        
    def test_validate_push_event(self):
        """Test push event validation."""
        # Valid push event
        valid_event = {
            "payload": {
                "commits": [
                    {
                        "sha": "abc123",
                        "message": "test commit",
                        "author": {"name": "User", "email": "user@test.com"}
                    }
                ]
            }
        }
        
        self.assertTrue(self.validator._validate_push_event(valid_event))
        
        # Invalid push event
        invalid_event = {
            "payload": {}
        }
        
        self.assertFalse(self.validator._validate_push_event(invalid_event))
        
    def test_validate_pull_request_event(self):
        """Test pull request event validation."""
        # Valid PR event
        valid_event = {
            "payload": {
                "pull_request": {
                    "id": 1,
                    "number": 100,
                    "title": "Test PR",
                    "state": "open"
                }
            }
        }
        
        self.assertTrue(self.validator._validate_pull_request_event(valid_event))
        
        # Invalid PR event
        invalid_event = {
            "payload": {
                "pull_request": {
                    "id": 1
                }
            }
        }
        
        self.assertFalse(self.validator._validate_pull_request_event(invalid_event))
        
    def test_validate_issue_event(self):
        """Test issue event validation."""
        # Valid issue event
        valid_event = {
            "payload": {
                "issue": {
                    "id": 1,
                    "number": 100,
                    "title": "Test Issue",
                    "state": "open"
                }
            }
        }
        
        self.assertTrue(self.validator._validate_issue_event(valid_event))
        
        # Invalid issue event
        invalid_event = {
            "payload": {
                "issue": {
                    "id": 1
                }
            }
        }
        
        self.assertFalse(self.validator._validate_issue_event(invalid_event))
        
    def test_validate_fork_event(self):
        """Test fork event validation."""
        # Valid fork event
        valid_event = {
            "payload": {
                "forkee": {
                    "id": 1,
                    "full_name": "user/repo"
                }
            }
        }
        
        self.assertTrue(self.validator._validate_fork_event(valid_event))
        
        # Invalid fork event
        invalid_event = {
            "payload": {
                "forkee": {
                    "id": 1
                }
            }
        }
        
        self.assertFalse(self.validator._validate_fork_event(invalid_event))
        
    def test_validate_watch_event(self):
        """Test watch event validation."""
        # Valid watch event
        valid_event = {
            "payload": {
                "action": "started"
            }
        }
        
        self.assertTrue(self.validator._validate_watch_event(valid_event))
        
        # Invalid watch event
        invalid_event = {
            "payload": {}
        }
        
        self.assertFalse(self.validator._validate_watch_event(invalid_event))
        
    def test_validate_star_event(self):
        """Test star event validation."""
        # Valid star event
        valid_event = {
            "payload": {
                "action": "created"
            }
        }
        
        self.assertTrue(self.validator._validate_star_event(valid_event))
        
        # Invalid star event
        invalid_event = {
            "payload": {}
        }
        
        self.assertFalse(self.validator._validate_star_event(invalid_event))

if __name__ == '__main__':
    unittest.main()
