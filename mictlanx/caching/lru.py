from collections import OrderedDict
from option import Option,Some,NONE
from typing import TypeVar,OrderedDict as ODT,Tuple

T = TypeVar("T")

class LRUCache:
    def __init__(self, capacity: int=10):
        self.cache:ODT[str,T] = OrderedDict()
        self.capacity = capacity
    def get(self, key: str) -> Option[Tuple[str,T]]:
        if key not in self.cache:
            return NONE  # Indicate that the key is not present
        else:
            # Move the key to the end to show that it was recently used
            self.cache.move_to_end(key)
            return Some(self.cache[key])

    def put(self, key: str, value:T) -> Option[Tuple[str,T]]:
        if key in self.cache:
            # Move the key to the end to show that it was recently used
            self.cache.move_to_end(key)
        else:
            if len(self.cache) >= self.capacity:
                x = self.cache.popitem(last=False)
                # Remove the first (least recently used) item from the cache
                return Some(x)
                # print("EVICTED FIRST",x)
        self.cache[key] = value


# if __name__ =="__main__":
    # lru_cache = LRUCache(capacity=2)
    # x= lru_cache.put("k0",value=memoryview(b"k0"))
    # print("K0 PUT",x)
    # x=lru_cache.put("k1",value=memoryview(b"k1"))
    # y = lru_cache.get("k0")
    # y = lru_cache.get("k0")
    # y = lru_cache.get("k1")
    # print("K1 PUT",x)
    # x=lru_cache.put("k2",value=memoryview(b"k2"))
    # print("K2 PUT",x)