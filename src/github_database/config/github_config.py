"""GitHub API configuration module."""

import os
from dataclasses import dataclass
from typing import Optional

@dataclass
class GitHubConfig:
    """GitHub API configuration."""
    
    access_token: str
    api_url: str = "https://api.github.com"
    rate_limit_delay: float = 0.1  # Delay between API calls
    parallel_requests: int = 2  # Number of parallel requests
    retry_count: int = 3  # Number of retries for failed requests
    retry_delay: float = 1.0  # Delay between retries in seconds
    
    @classmethod
    def from_env(cls) -> 'GitHubConfig':
        """Create GitHub config from environment variables."""
        token = os.getenv('GITHUB_TOKEN')
        if not token:
            token = os.getenv('GITHUB_API_TOKEN')  # Fallback
            
        if not token:
            raise ValueError("GitHub token not found in environment variables. "
                           "Please set GITHUB_TOKEN or GITHUB_API_TOKEN.")
        
        return cls(
            access_token=token,
            api_url=os.getenv('GITHUB_API_URL', 'https://api.github.com'),
            rate_limit_delay=float(os.getenv('GITHUB_RATE_LIMIT_DELAY', '0.1')),
            parallel_requests=int(os.getenv('GITHUB_PARALLEL_REQUESTS', '2')),
            retry_count=int(os.getenv('GITHUB_RETRY_COUNT', '3')),
            retry_delay=float(os.getenv('GITHUB_RETRY_DELAY', '1.0'))
        )
