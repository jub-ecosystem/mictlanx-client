from typing import TypeVar,OrderedDict as ODT,Tuple,List,Dict
from abc import ABC, abstractmethod
from collections import OrderedDict, Counter
import heapq
from mictlanx.interfaces import Metadata
from option import Option, Some, NONE

T = TypeVar("T")




class CacheX(ABC):
    """Abstract Base Class for a simple key-value cache (Key: str -> Value: bytes/memoryview)."""
    @abstractmethod
    def get_keys(self)->List[str]:
        """Return all keys currently stored in the cache.

        Returns:
            A list of cache key strings.
        """
        pass
    @abstractmethod
    def get(self, key: str) -> Option[memoryview]:
        """Retrieve a value from the cache."""
        pass

    @abstractmethod
    def put(self, key: str, value: bytes)->int:
        """Insert a value into the cache."""
        pass

    @abstractmethod
    def remove(self, key: str):
        """Remove a value from the cache."""
        pass

    @abstractmethod
    def __len__(self) -> int:
        """Return the current size of the cache."""
        pass

    @abstractmethod
    def clear(self):
        """Clear the cache."""
        pass
    @abstractmethod
    def get_total_storage_capacity(self):
        """Return the maximum byte capacity of this cache.

        Returns:
            Total capacity in bytes.
        """
        pass

    @abstractmethod
    def get_used_storage_capacity(self):
        """Return the number of bytes currently consumed by cached values.

        Returns:
            Used capacity in bytes.
        """
        pass

    @abstractmethod
    def get_uf(self):
        """Return the utilisation factor of this cache (0.0 = empty, 1.0 = full).

        Returns:
            Float in ``[0.0, 1.0]`` representing the fraction of capacity used.
        """
        pass

class CacheFactory:
    """Factory for creating byte-budget-bounded in-memory caches."""

    @staticmethod
    def create(eviction_policy:str,capacity_storage:int):
        """Instantiate a cache with the given eviction policy and byte budget.

        Args:
            eviction_policy: ``"LRU"`` for least-recently-used eviction or
                ``"LFU"`` for least-frequently-used.  Any other value falls
                back to LRU.
            capacity_storage: Maximum number of bytes the cache may hold
                before evicting entries.

        Returns:
            A ``CacheX`` instance (``LRUCache`` or ``LFUCache``).
        """
        if eviction_policy == "LRU":
            return LRUCache(capacity_storage = capacity_storage)
        elif eviction_policy == "LFU":
            return LFUCache(capacity_storage = capacity_storage)
        return LRUCache(capacity_storage=capacity_storage)


class LRUCache(CacheX):
    """LRU (Least Recently Used) Cache implementation using OrderedDict."""

    def __init__(self, capacity_storage:int):
        # self.capacity         = capacity
        self.capacity_storage = capacity_storage
        self.used_capacity    = 0
        self.cache:ODT[str,Tuple[Metadata,memoryview]]            = OrderedDict()  # Maintains insertion order

    def get_keys(self):
        """Return all keys currently held in the LRU cache.

        Returns:
            List of key strings in insertion order.
        """
        return list(self.cache.keys())

    def get(self, key: str) ->Option[Tuple[Metadata,memoryview]]:
        """Retrieve a cached value and promote it to most-recently-used.

        Args:
            key: Cache key to look up.

        Returns:
            ``Some((metadata, memoryview))`` if the key exists, else ``NONE``.
        """
        if key in self.cache:
            self.cache.move_to_end(key)  # Mark as recently used
            return Some(self.cache[key])
        return NONE  # Key not found

    def put(self, key: str, value: bytes, metadata:Metadata)->int:
        """Insert or refresh a value in the cache.

        If the new entry would exceed the byte budget the least-recently-used
        entry is evicted first.

        Args:
            key: Cache key.
            value: Raw bytes to store.
            metadata: ``Metadata`` object associated with the value.

        Returns:
            ``0`` on success, ``-1`` on error.
        """
        try:
            size                  = len(value)
            current_used_capacity = self.used_capacity + size
            can_store             = current_used_capacity <= self.capacity_storage
            if key in self.cache:
                self.cache.move_to_end(key)  # Mark as recently used
            # elif len(self.cache) >= self.capacity :
            elif not can_store:
                (_,deleted_value) = self.cache.popitem(last=False)  # Remove the least recently used item
                self.used_capacity-=len(deleted_value[1])

            self.cache[key] = (metadata,memoryview(value))  # Store new value
            self.used_capacity+= size
            return 0
        except Exception as e:
            print(e)
            return -1

    def remove(self, key: str):
        """Remove a key from the cache and reclaim its byte budget.

        Args:
            key: Cache key to remove.  No-op if the key is not present.
        """
        if key in self.cache:
            element = self.cache[key]
            self.used_capacity -= len(element[1])
            del self.cache[key]

    def __len__(self) -> int:
        return len(self.cache)

    def clear(self):
        """Remove all entries from the cache and reset the byte counter."""
        self.cache.clear()
        self.used_capacity= 0

    def get_total_storage_capacity(self):
        """Return the maximum byte capacity of the LRU cache.

        Returns:
            Total capacity in bytes.
        """
        return self.capacity_storage

    def get_used_storage_capacity(self):
        """Return the number of bytes currently occupied by cached values.

        Returns:
            Used capacity in bytes.
        """
        return self.used_capacity

    def get_uf(self):
        """Return the cache utilisation factor (0.0 = empty, 1.0 = full).

        Returns:
            Float in ``[0.0, 1.0]``.
        """
        return 1- ((self.get_total_storage_capacity() - self.get_used_storage_capacity())/self.get_total_storage_capacity())


class LFUCache(CacheX):
    """LFU (Least Frequently Used) Cache implementation using a frequency counter and heap."""

    def __init__(self, capacity_storage:int):
        self.capacity_storage = capacity_storage
        self.used_capacity    = 0
        self.cache:Dict[str, Tuple[Metadata, bytes]] = {}  # Key -> Value (bytes)
        self.freq_counter = Counter()  # Key -> Frequency
        self.freq_heap = []  # Min-heap to track least frequently used keys

    def get_keys(self):
        """Return all keys currently held in the LFU cache.

        Returns:
            List of key strings.
        """
        return list(self.cache.keys())

    def get(self, key: str) -> Option[Tuple[Metadata,memoryview]]:
        """Retrieve a cached value and increment its access frequency.

        Args:
            key: Cache key to look up.

        Returns:
            ``Some((metadata, memoryview))`` if the key exists, else ``NONE``.
        """
        if key in self.cache:
            self.freq_counter[key] += 1
            heapq.heappush(self.freq_heap, (self.freq_counter[key], key))
            return Some(self.cache[key])
        return NONE

    def put(self, key: str, value: bytes,metadata:Metadata) -> int:
        """Insert or update a value in the cache.

        If the entry would exceed the byte budget the least-frequently-used
        entry is evicted first.

        Args:
            key: Cache key.
            value: Raw bytes to store.
            metadata: ``Metadata`` object associated with the value.

        Returns:
            ``0`` on success, ``-1`` on error.
        """
        try:
            size                  = len(value)
            current_used_capacity = self.used_capacity + size
            can_store             = current_used_capacity <= self.capacity_storage
            if key in self.cache:
                old_value = self.cache[key][1]
                old_size = len(old_value)
                if old_size != size:
                    self.used_capacity-= size
                    self.used_capacity+= old_size
                

                self.cache[key] = (metadata,value)
                self.freq_counter[key] += 1
            else:
                # if len(self.cache) >= self.capacity:
                if not can_store:
                    # Remove the least frequently used item
                    while self.freq_heap:
                        freq, least_used_key = heapq.heappop(self.freq_heap)
                        if self.freq_counter[least_used_key] == freq:
                            (least_metadata, least_value) = self.cache[least_used_key]
                            self.used_capacity-= len(least_value)
                            del self.cache[least_used_key]
                            del self.freq_counter[least_used_key]
                            break
                
                
                self.cache[key] = (metadata,value)
                self.freq_counter[key] = 1
                self.used_capacity+= size
            heapq.heappush(self.freq_heap, (self.freq_counter[key], key))
            return 0
        except Exception:
            return -1

    def remove(self, key: str):
        """Remove a key from the cache and reclaim its byte budget.

        Args:
            key: Cache key to remove.  No-op if the key is not present.
        """
        if key in self.cache:
            element = self.cache[key]
            self.used_capacity-= len(element[1])
            del self.cache[key]
            del self.freq_counter[key]

    def __len__(self) -> int:
        return len(self.cache)

    def clear(self):
        """Remove all entries from the cache and reset frequency tracking."""
        self.cache.clear()
        self.freq_counter.clear()
        self.freq_heap.clear()
        self.used_capacity = 0

    def get_total_storage_capacity(self):
        """Return the maximum byte capacity of the LFU cache.

        Returns:
            Total capacity in bytes.
        """
        return self.capacity_storage

    def get_used_storage_capacity(self):
        """Return the number of bytes currently occupied by cached values.

        Returns:
            Used capacity in bytes.
        """
        return self.used_capacity

    def get_uf(self):
        """Return the cache utilisation factor (0.0 = empty, 1.0 = full).

        Returns:
            Float in ``[0.0, 1.0]``.
        """
        return 1- ((self.get_total_storage_capacity() - self.get_used_storage_capacity())/self.get_total_storage_capacity())

class NoCache(CacheX):
    """No-op cache that never stores anything.

    Useful as a drop-in when caching should be disabled without changing
    calling code.  All lookups return ``NONE``; all writes are ignored.
    """

    def get(self, key):
        return NONE

    def put(self, key, value, metadata=None):
        return None

    def remove(self, key):
        pass

    def clear(self):
        pass

    def __len__(self):
        return 0

    def get_keys(self) -> List[str]:
        return []

    def get_total_storage_capacity(self) -> int:
        return 0

    def get_used_storage_capacity(self) -> int:
        return 0

    def get_uf(self) -> float:
        return 0.0


