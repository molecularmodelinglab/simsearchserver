"""Expiring result cache, so a repeated query skips the model and the index."""

import threading
import time


class ResultCache:
    """Bounded and time-limited, so no one's query is held indefinitely."""

    def __init__(self, ttl_seconds, max_entries):
        self.ttl = ttl_seconds
        self.max_entries = max_entries
        self._entries = {}
        self._lock = threading.Lock()
        self.hits = 0
        self.misses = 0

    def get(self, key):
        now = time.monotonic()

        with self._lock:
            found = self._entries.get(key)

            if found is None:
                self.misses += 1
                return None

            stored_at, value = found

            if now - stored_at > self.ttl:
                del self._entries[key]
                self.misses += 1
                return None

            self.hits += 1

            return value

    def put(self, key, value):
        now = time.monotonic()

        with self._lock:
            self._entries[key] = (now, value)
            self._evict(now)

    def _evict(self, now):
        expired = [k for k, (stored_at, _) in self._entries.items()
                   if now - stored_at > self.ttl]
        for key in expired:
            del self._entries[key]

        # Oldest first once the cap is reached.
        while len(self._entries) > self.max_entries:
            oldest = min(self._entries, key=lambda k: self._entries[k][0])
            del self._entries[oldest]

    def stats(self):
        with self._lock:
            return {"entries": len(self._entries), "hits": self.hits,
                    "misses": self.misses, "ttl_seconds": self.ttl}
