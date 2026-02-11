import pytest
from mictlanx.caching import CacheFactory
from mictlanx.interfaces.responses import Metadata 
import time as T

@pytest.fixture
def sample_metadata():
    """Returns a valid Metadata instance for testing."""
    return Metadata(
        key="test_key",
        size=10,
        checksum="sha256_hash",
        tags={"env": "test"},
        content_type="application/octet-stream",
        producer_id="user_123",
        ball_id="segment_abc"
    )

@pytest.fixture
def lru_cache():
    """Fresh LRU cache instance with 100 bytes capacity."""
    return CacheFactory.create("LRU", capacity_storage=100)

@pytest.fixture
def lfu_cache():
    """Fresh LFU cache instance with 100 bytes capacity."""
    return CacheFactory.create("LFU", capacity_storage=100)

# --- Test Functions ---

def test_cache_input_output_types(lru_cache, sample_metadata):
    """Verify bytes go in and memoryview comes out."""
    data = b"cache_data"
    lru_cache.put("k1", data, sample_metadata)
    
    result = lru_cache.get("k1")
    assert result.is_some
    
    meta, val = result.unwrap()
    assert isinstance(val, memoryview)
    assert val.tobytes() == data
    assert meta.key == "test_key"

def test_lru_eviction(lru_cache, sample_metadata):
    """Verify the Least Recently Used item is dropped."""
    lru_cache.capacity_storage = 20  # Small capacity
    
    lru_cache.put("first", b"1234567890", sample_metadata) # 10 bytes
    lru_cache.put("second", b"1234567890", sample_metadata) # 10 bytes
    
    # Touch 'first' to make it recently used
    lru_cache.get("first")
    
    # Adding 'third' (10 bytes) must evict 'second'
    lru_cache.put("third", b"1234567890", sample_metadata)
    
    assert lru_cache.get("first").is_some
    assert lru_cache.get("second").is_none
    assert lru_cache.get("third").is_some

def test_lfu_eviction(lfu_cache, sample_metadata):
    """Verify the Least Frequently Used item is dropped."""
    lfu_cache.capacity_storage = 20
    
    lfu_cache.put("popular", b"10bytes___", sample_metadata)
    lfu_cache.put("ignored", b"10bytes___", sample_metadata)
    
    # Increase frequency
    lfu_cache.get("popular")
    lfu_cache.get("popular")
    
    # This should evict 'ignored'
    lfu_cache.put("new", b"10bytes___", sample_metadata)
    
    assert lfu_cache.get("popular").is_some
    assert lfu_cache.get("ignored").is_none

def test_utilization_factor(lru_cache, sample_metadata):
    """Confirm the UF calculation is accurate."""
    lru_cache.capacity_storage = 100
    lru_cache.put("data", b"a" * 75, sample_metadata)
    
    # 75/100 = 0.75
    assert lru_cache.get_uf() == 0.75

def test_clear_resets_state(lru_cache, sample_metadata):
    """Ensure clear() wipes data and resets usage."""
    lru_cache.put("key", b"some_data", sample_metadata)
    lru_cache.clear()
    
    assert len(lru_cache) == 0
    assert lru_cache.get_used_storage_capacity() == 0