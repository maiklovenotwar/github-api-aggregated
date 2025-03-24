"""Token-Pool für die Verwaltung mehrerer GitHub API-Tokens."""

import threading
import time
import logging
from typing import List, Tuple, Optional

logger = logging.getLogger(__name__)

class TokenPool:
    """Pool von GitHub API-Tokens mit Ratenbegrenzungsverwaltung."""
    
    def __init__(self, tokens: List[str]):
        """
        Initialisiere Token-Pool.
        
        Args:
            tokens: Liste von GitHub API-Tokens
        """
        if not tokens:
            raise ValueError("Token-Pool benötigt mindestens einen Token")
            
        self.tokens = tokens
        self.current_index = 0
        self.locks = [threading.Lock() for _ in tokens]
        self.rate_limits = [5000 for _ in tokens]  # GitHub API-Limit: 5000 Anfragen pro Stunde
        self.reset_times = [time.time() + 3600 for _ in tokens]
        
        logger.info(f"Token-Pool mit {len(tokens)} Tokens initialisiert")
        
    def get_token(self) -> Tuple[str, int]:
        """
        Hole verfügbaren Token aus dem Pool.
        
        Returns:
            Tuple aus Token und Index im Pool
        """
        for i in range(len(self.tokens)):
            idx = (self.current_index + i) % len(self.tokens)
            with self.locks[idx]:
                if self.rate_limits[idx] > 0 or time.time() > self.reset_times[idx]:
                    if time.time() > self.reset_times[idx]:
                        # Reset-Zeit ist abgelaufen, setze Rate-Limit zurück
                        self.rate_limits[idx] = 5000
                        self.reset_times[idx] = time.time() + 3600
                        logger.info(f"Token {idx} zurückgesetzt: {self.rate_limits[idx]} Anfragen verfügbar")
                    
                    # Reduziere das Rate-Limit für diesen Token
                    self.rate_limits[idx] -= 1
                    
                    # Aktualisiere den aktuellen Index für Round-Robin
                    self.current_index = (idx + 1) % len(self.tokens)
                    
                    return self.tokens[idx], idx
        
        # Alle Tokens sind erschöpft, warten auf den nächsten Reset
        min_reset = min(self.reset_times)
        wait_time = max(0, min_reset - time.time())
        logger.warning(f"Alle Tokens erschöpft. Warte {wait_time:.1f}s auf Reset.")
        time.sleep(wait_time)
        return self.get_token()
    
    def update_rate_limit(self, token_idx: int, remaining: int, reset_time: float) -> None:
        """
        Aktualisiere Ratenbegrenzungsinformationen für einen Token.
        
        Args:
            token_idx: Index des Tokens im Pool
            remaining: Verbleibende Anfragen
            reset_time: Unix-Timestamp für Reset-Zeit
        """
        with self.locks[token_idx]:
            self.rate_limits[token_idx] = remaining
            self.reset_times[token_idx] = reset_time
            logger.debug(f"Token {token_idx} aktualisiert: {remaining} Anfragen verbleibend, Reset um {reset_time}")
    
    def get_stats(self) -> List[dict]:
        """
        Hole Statistiken für alle Tokens.
        
        Returns:
            Liste von Dictionaries mit Statistiken für jeden Token
        """
        stats = []
        for i in range(len(self.tokens)):
            with self.locks[i]:
                stats.append({
                    "index": i,
                    "rate_limit_remaining": self.rate_limits[i],
                    "reset_time": self.reset_times[i],
                    "reset_in_seconds": max(0, self.reset_times[i] - time.time())
                })
        return stats
