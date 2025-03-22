"""Database configuration."""

from dataclasses import dataclass

@dataclass
class DatabaseConfig:
    """Database configuration."""
    url: str = "sqlite:///github_database.db"
    batch_size: int = 500
    max_retries: int = 3
    retry_delay: int = 5
