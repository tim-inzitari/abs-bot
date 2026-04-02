import time
from typing import Dict, Generic, Optional, TypeVar

T = TypeVar("T")


class TTLCache(Generic[T]):
    def __init__(self, ttl_seconds: int) -> None:
        self.ttl_seconds = ttl_seconds
        self._store: Dict[str, T] = {}
        self._expires_at: Dict[str, float] = {}

    def get(self, key: str) -> Optional[T]:
        expires_at = self._expires_at.get(key)
        if expires_at is None:
            return None
        if time.time() > expires_at:
            self._store.pop(key, None)
            self._expires_at.pop(key, None)
            return None
        return self._store.get(key)

    def set(self, key: str, value: T) -> None:
        self._store[key] = value
        self._expires_at[key] = time.time() + self.ttl_seconds
