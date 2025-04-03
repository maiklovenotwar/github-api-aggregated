"""
Simplified Cache Implementation for API Requests.

This module implements a simple memory caching strategy for API requests 
to reduce the number of network requests and improve performance.
"""

import time
import logging
from typing import Dict, Any, Optional, Callable, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar('T')  # Type variable for generic functions

class MemoryCache:
    """
    Simple in-memory cache with size limitation.
    
    Implements an LRU-like cache (Least Recently Used) that removes the least
    recently used entries when the maximum size is reached.
    """
    
    def __init__(self, name: str, max_size: int = 1000, max_age: int = 86400):
        """
        Initialize cache.
        
        Args:
            name: Name of the cache (for logging)
            max_size: Maximum number of entries to store
            max_age: Maximum age of cache entries in seconds
        """
        self.name = name
        self.max_size = max_size
        self.max_age = max_age
        self.cache: Dict[str, Dict[str, Any]] = {}
        self.access_times: Dict[str, float] = {}
        logger.info(f"Memory cache '{name}' initialized (max_size={max_size}, max_age={max_age}s)")
    
    def get(self, key: str) -> Optional[Any]:
        """
        Get value from cache.
        
        Args:
            key: Cache key
            
        Returns:
            Stored value or None if not found or expired
        """
        if key in self.cache:
            entry = self.cache[key]
            current_time = time.time()
            
            # Check if entry has expired
            if current_time - entry['timestamp'] > self.max_age:
                logger.debug(f"Cache entry '{key}' in '{self.name}' has expired")
                self.remove(key)
                return None
            
            # Update access time
            self.access_times[key] = current_time
            logger.debug(f"Cache hit for '{key}' in '{self.name}'")
            return entry['value']
            
        logger.debug(f"Cache miss for '{key}' in '{self.name}'")
        return None
    
    def set(self, key: str, value: Any) -> None:
        """
        Store value in cache.
        
        Args:
            key: Cache key
            value: Value to store
        """
        current_time = time.time()
        
        # Check cache size and remove oldest entry if necessary
        if len(self.cache) >= self.max_size and key not in self.cache:
            self._remove_oldest_entry()
        
        self.cache[key] = {
            'value': value,
            'timestamp': current_time
        }
        self.access_times[key] = current_time
        logger.debug(f"Value for '{key}' cached in '{self.name}'")
    
    def remove(self, key: str) -> None:
        """
        Remove entry from cache.
        
        Args:
            key: Cache key to remove
        """
        if key in self.cache:
            del self.cache[key]
            if key in self.access_times:
                del self.access_times[key]
            logger.debug(f"Entry '{key}' removed from '{self.name}'")
    
    def clear(self) -> None:
        """Clear the entire cache."""
        self.cache.clear()
        self.access_times.clear()
        logger.info(f"Cache '{self.name}' cleared")
    
    def _remove_oldest_entry(self) -> None:
        """Remove the least recently used entry."""
        if not self.access_times:
            return
            
        oldest_key = min(self.access_times.items(), key=lambda x: x[1])[0]
        self.remove(oldest_key)
        logger.debug(f"Oldest entry '{oldest_key}' removed from '{self.name}'")


def cached(cache_instance, key_prefix: str = ""):
    """
    Decorator for caching function calls.
    
    Args:
        cache_instance: Instance of MemoryCache
        key_prefix: Optional prefix for cache keys
        
    Returns:
        Decorated function with caching
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            # Create a simple cache key from function name and arguments
            key_parts = [key_prefix, func.__name__]
            for arg in args:
                key_parts.append(str(arg))
            for k, v in sorted(kwargs.items()):
                key_parts.append(f"{k}={v}")
            
            cache_key = ":".join(key_parts)
            
            # Try to get from cache
            cached_result = cache_instance.get(cache_key)
            if cached_result is not None:
                return cached_result
            
            # Execute function and cache result
            result = func(*args, **kwargs)
            cache_instance.set(cache_key, result)
            return result
        
        return wrapper
    
    return decorator
