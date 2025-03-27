"""
Error classes and helper classes for API access.

This module defines common error classes for API operations,
specifically for GitHub API.
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)

class APIError(Exception):
    """Base class for all API-related errors."""
    
    def __init__(self, message: str, status_code: Optional[int] = None):
        """Initialize API error."""
        super().__init__(message)
        self.status_code = status_code
        self.message = message


class GitHubAPIError(APIError):
    """Error in GitHub API requests."""
    
    def __init__(self, message: str, status_code: Optional[int] = None, 
                 response_data: Optional[dict] = None):
        """
        Initialize GitHub API error.
        
        Args:
            message: Error message
            status_code: HTTP status code
            response_data: Optional API response data for debugging
        """
        super().__init__(message, status_code)
        self.response_data = response_data


class RateLimitError(GitHubAPIError):
    """GitHub API rate limit error."""
    
    def __init__(self, message: str, reset_time: float, 
                 status_code: int = 403, response_data: Optional[dict] = None):
        """
        Initialize rate limit error.
        
        Args:
            message: Error message
            reset_time: Unix timestamp when the rate limit will be reset
            status_code: HTTP status code (typically 403)
            response_data: Optional API response data for debugging
        """
        super().__init__(message, status_code, response_data)
        self.reset_time = reset_time


class AuthenticationError(GitHubAPIError):
    """Error in authentication with the GitHub API."""
    
    def __init__(self, message: str, status_code: int = 401, 
                 token_id: Optional[int] = None, response_data: Optional[dict] = None):
        """
        Initialize authentication error.
        
        Args:
            message: Error message
            status_code: HTTP status code (typically 401)
            token_id: Optional token ID for debugging
            response_data: Optional API response data for debugging
        """
        super().__init__(message, status_code, response_data)
        self.token_id = token_id


class NotFoundError(GitHubAPIError):
    """Resource was not found."""
    
    def __init__(self, message: str, resource_type: str, 
                 resource_id: str, status_code: int = 404):
        """
        Initialize not found error.
        
        Args:
            message: Error message
            resource_type: Type of resource not found (e.g., 'repository', 'user')
            resource_id: ID or name of the resource not found
            status_code: HTTP status code (typically 404)
        """
        super().__init__(message, status_code)
        self.resource_type = resource_type
        self.resource_id = resource_id
