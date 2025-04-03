"""
Token pool for efficient management of multiple GitHub API tokens.

This module implements a thread-safe token pool that manages multiple GitHub 
API tokens and automatically switches between them to bypass rate limits.
Each token is tracked with its current rate limit status.
"""

import threading
import time
import logging
from typing import List, Dict, Any

# Configure logger
logger = logging.getLogger(__name__)

class TokenPool:
    """
    Pool of GitHub API tokens with rate limit management.
    
    The TokenPool implements a round-robin strategy for using multiple
    tokens and monitors their rate limit status. When a token reaches its limit,
    the pool automatically switches to the next available token.
    
    Attributes:
        tokens: List of API tokens
        current_index: Current token index for round-robin
        locks: Thread locks for each token
        rate_limits: Remaining requests for each token
        reset_times: Reset times for each token
    """
    
    def __init__(self, tokens: List[str], default_rate_limit: int = 5000):
        """
        Initialize token pool.
        
        Args:
            tokens: List of GitHub API tokens
            default_rate_limit: Default rate limit for each token (typically 5000/hour)
        
        Raises:
            ValueError: If no tokens are provided
        """
        if not tokens:
            raise ValueError("Token pool requires at least one token")
            
        self.tokens = tokens
        self.current_index = 0
        self.locks = [threading.Lock() for _ in tokens]
        self.rate_limits = [default_rate_limit for _ in tokens]
        self.reset_times = [time.time() + 3600 for _ in tokens]
        self.usage_count = [0 for _ in tokens]  # Counter for token usage
        
        logger.info(f"Token pool initialized with {len(tokens)} tokens")
        
    def get_token(self) -> str:
        """
        Get available token from the pool.
        
        This method implements a thread-safe strategy to find the next
        available token. If all tokens are exhausted, the method
        automatically waits for the next reset.
        
        Returns:
            Token string
        """
        start_idx = self.current_index
        for i in range(len(self.tokens)):
            idx = (start_idx + i) % len(self.tokens)
            
            with self.locks[idx]:
                # Check if the token is available or should be reset
                current_time = time.time()
                if self.rate_limits[idx] > 0 or current_time > self.reset_times[idx]:
                    if current_time > self.reset_times[idx]:
                        # Reset time has passed, reset rate limit
                        self.rate_limits[idx] = 5000
                        self.reset_times[idx] = current_time + 3600
                        logger.info(f"Token {idx} reset: {self.rate_limits[idx]} requests available")
                    
                    # Reduce the rate limit for this token
                    self.rate_limits[idx] -= 1
                    self.usage_count[idx] += 1
                    
                    # Update the current index for round-robin
                    self.current_index = (idx + 1) % len(self.tokens)
                    
                    return self.tokens[idx]
        
        # All tokens are exhausted, wait for the next reset
        min_reset = min(self.reset_times)
        wait_time = max(0, min_reset - time.time())
        
        if wait_time > 0:
            logger.warning(f"All tokens exhausted. Waiting {wait_time:.1f}s for reset.")
            time.sleep(wait_time)
        
        return self.get_token()  # Recursive call after waiting
    
    def update_token_usage(self, token: str, remaining: int, reset_time: float) -> None:
        """
        Update rate limit information for a token.
        
        This method is typically called after an API request to update the
        rate limit information returned by the GitHub server.
        
        Args:
            token: The token string that was used
            remaining: Remaining requests
            reset_time: Unix timestamp for reset time
        """
        try:
            token_idx = self.tokens.index(token)
        except ValueError:
            logger.error(f"Token not found in pool")
            return
            
        with self.locks[token_idx]:
            old_remaining = self.rate_limits[token_idx]
            self.rate_limits[token_idx] = remaining
            self.reset_times[token_idx] = reset_time
            
            # Only log if value changed significantly
            if abs(old_remaining - remaining) > 10 or remaining <= 100:
                logger.info(f"Token {token_idx}: {remaining} requests remaining, reset at {time.ctime(reset_time)}")
            else:
                logger.debug(f"Token {token_idx}: {remaining} requests remaining")
    
    def get_stats(self) -> List[Dict[str, Any]]:
        """
        Get statistics for all tokens.
        
        Returns detailed statistics for each token, including
        current rate limits, reset times, and usage counters.
        
        Returns:
            List of dictionaries with statistics for each token
        """
        current_time = time.time()
        stats = []
        
        for i in range(len(self.tokens)):
            with self.locks[i]:
                token_masked = f"{self.tokens[i][:4]}...{self.tokens[i][-4:]}"
                stats.append({
                    "index": i,
                    "token": token_masked,
                    "rate_limit_remaining": self.rate_limits[i],
                    "reset_time": self.reset_times[i],
                    "reset_in_seconds": max(0, self.reset_times[i] - current_time),
                    "usage_count": self.usage_count[i],
                    "status": "active" if self.rate_limits[i] > 0 else "exhausted"
                })
                
        return stats
    
    @classmethod
    def from_config(cls, github_config):
        """
        Create TokenPool from GitHub configuration.
        
        Args:
            github_config: GitHubConfig object with access_token and additional_tokens
            
        Returns:
            TokenPool instance or None if no token pool should be used
        """
        if not github_config.use_token_pool:
            return None
            
        # Combine main token and additional tokens
        all_tokens = [github_config.access_token] + github_config.additional_tokens
        
        # Remove duplicates (if any)
        unique_tokens = list(dict.fromkeys(all_tokens))
        
        if len(unique_tokens) <= 1:
            logger.warning("Only one token found, token pool will not be used")
            return None
            
        return cls(unique_tokens)
