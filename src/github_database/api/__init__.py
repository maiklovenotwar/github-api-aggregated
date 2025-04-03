"""
API package for GitHub data ETL.

This package provides interfaces for external data sources, particularly
the GitHub API. It implements:

1. GitHub API client with token pool and caching
2. Helper classes for rate limiting and error handling
3. Simple GitHub client without complex dependencies
"""

from .token_pool import TokenPool
from .github_api import (
    GitHubAPIClient, 
    GitHubAPIError, 
    RateLimitError, 
    create_repository_from_api,
    create_user_from_api,
    create_organization_from_api
)
from .simple_github_client import SimpleGitHubClient

__all__ = [
    'TokenPool',
    'GitHubAPIClient',
    'GitHubAPIError',
    'RateLimitError',
    'create_repository_from_api',
    'create_user_from_api',
    'create_organization_from_api',
    'SimpleGitHubClient'
]