"""
Configuration package for GitHub data ETL.

This package provides a unified configuration interface for all components of the
GitHub data ETL system.
"""

from .config import (
    GitHubConfig, 
    QualityConfig, 
    CacheConfig, 
    ETLConfig, 
    load_config
)

__all__ = [
    'GitHubConfig', 
    'QualityConfig', 
    'CacheConfig', 
    'ETLConfig', 
    'load_config'
]
