"""Test user location handling in the database."""

import os
import unittest
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.github_database.database.database import Base, User
from src.github_database.mapping.repository_mapper import RepositoryMapper
from src.github_database.config import ETLConfig

class TestUserLocation(unittest.TestCase):
    """Test user location data handling."""
    
    def setUp(self):
        """Set up test database."""
        # Create in-memory database for testing
        self.engine = create_engine('sqlite:///:memory:')
        Base.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
        
        # Create test config with date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)  # Test with last 30 days
        self.config = ETLConfig(start_date=start_date, end_date=end_date)
        
        # Create repository mapper
        self.mapper = RepositoryMapper(self.session, self.config, None)
        
    def tearDown(self):
        """Clean up after tests."""
        self.session.close()
        
    def test_user_creation_with_location(self):
        """Test creating a user with location data."""
        # Test data
        user_id = 1
        user_login = "testuser"
        enriched_data = {
            "name": "Test User",
            "email": "test@example.com",
            "location": "Berlin, Germany",
            "location_data": {
                "latitude": "52.5200",
                "longitude": "13.4050",
                "country": "Germany",
                "city": "Berlin"
            },
            "company": "Test Corp",
            "blog": "https://test.com",
            "bio": "Test bio",
            "type": "User",
            "site_admin": False,
            "hireable": True,
            "public_repos": 10,
            "public_gists": 5,
            "followers": 100,
            "following": 50,
            "created_at": datetime.now(),
            "updated_at": datetime.now()
        }
        
        # Create user using repository mapper
        user = self.mapper._extract_user(user_id, user_login, enriched_data)
        
        # Verify all fields are set correctly
        self.assertEqual(user.id, user_id)
        self.assertEqual(user.login, user_login)
        self.assertEqual(user.name, enriched_data["name"])
        self.assertEqual(user.email, enriched_data["email"])
        self.assertEqual(user.location, enriched_data["location"])
        self.assertEqual(user.location_lat, enriched_data["location_data"]["latitude"])
        self.assertEqual(user.location_lon, enriched_data["location_data"]["longitude"])
        self.assertEqual(user.location_country, enriched_data["location_data"]["country"])
        self.assertEqual(user.location_city, enriched_data["location_data"]["city"])
        self.assertEqual(user.company, enriched_data["company"])
        self.assertEqual(user.blog, enriched_data["blog"])
        self.assertEqual(user.bio, enriched_data["bio"])
        self.assertEqual(user.type, enriched_data["type"])
        self.assertEqual(user.site_admin, enriched_data["site_admin"])
        self.assertEqual(user.hireable, enriched_data["hireable"])
        self.assertEqual(user.public_repos, enriched_data["public_repos"])
        self.assertEqual(user.public_gists, enriched_data["public_gists"])
        self.assertEqual(user.followers, enriched_data["followers"])
        self.assertEqual(user.following, enriched_data["following"])
        
    def test_user_creation_without_location(self):
        """Test creating a user without location data."""
        # Test data
        user_id = 2
        user_login = "testuser2"
        
        # Create user using repository mapper without enriched data
        user = self.mapper._extract_user(user_id, user_login)
        
        # Verify basic fields are set and location fields are None
        self.assertEqual(user.id, user_id)
        self.assertEqual(user.login, user_login)
        self.assertIsNone(user.location)
        self.assertIsNone(user.location_lat)
        self.assertIsNone(user.location_lon)
        self.assertIsNone(user.location_country)
        self.assertIsNone(user.location_city)
        
    def test_user_update_with_location(self):
        """Test updating a user with location data."""
        # First create user without location
        user_id = 3
        user_login = "testuser3"
        user = self.mapper._extract_user(user_id, user_login)
        self.session.commit()
        
        # Now update with location data
        enriched_data = {
            "location": "Munich, Germany",
            "location_data": {
                "latitude": "48.1351",
                "longitude": "11.5820",
                "country": "Germany",
                "city": "Munich"
            }
        }
        
        # Update user
        updated_user = self.mapper._extract_user(user_id, user_login, enriched_data)
        self.session.commit()
        
        # Verify location fields are updated
        self.assertEqual(updated_user.location, enriched_data["location"])
        self.assertEqual(updated_user.location_lat, enriched_data["location_data"]["latitude"])
        self.assertEqual(updated_user.location_lon, enriched_data["location_data"]["longitude"])
        self.assertEqual(updated_user.location_country, enriched_data["location_data"]["country"])
        self.assertEqual(updated_user.location_city, enriched_data["location_data"]["city"])

if __name__ == '__main__':
    unittest.main()
