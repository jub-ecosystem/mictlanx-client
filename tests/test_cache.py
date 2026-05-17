import pytest
from mictlanx.caching import CacheFactory, LRUCache, LFUCache, NoCache
from mictlanx.interfaces.responses import Metadata

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

# --- NoCache ---

def test_no_cache_get_always_none(no_cache):
    assert no_cache.get("any_key").is_none

def test_no_cache_put_is_noop(no_cache, sample_metadata):
    result = no_cache.put("k", b"data", sample_metadata)
    assert result is None
    assert no_cache.get("k").is_none

def test_no_cache_len_is_zero(no_cache):
    assert len(no_cache) == 0

def test_no_cache_remove_noop(no_cache):
    no_cache.remove("nonexistent")  # must not raise

def test_no_cache_clear_noop(no_cache):
    no_cache.clear()
    assert len(no_cache) == 0

def test_no_cache_capacities(no_cache):
    assert no_cache.get_total_storage_capacity() == 0
    assert no_cache.get_used_storage_capacity() == 0
    assert no_cache.get_uf() == 0.0
    assert no_cache.get_keys() == []

# --- LRU extras ---

def test_lru_remove_explicit(lru_cache, sample_metadata):
    lru_cache.put("k1", b"data", sample_metadata)
    lru_cache.remove("k1")
    assert lru_cache.get("k1").is_none
    assert lru_cache.get_used_storage_capacity() == 0

def test_lru_storage_capacity(lru_cache):
    assert lru_cache.get_total_storage_capacity() == 100

def test_lru_same_key_reinsertion(lru_cache, sample_metadata):
    lru_cache.put("k", b"hello", sample_metadata)
    lru_cache.put("k", b"world", sample_metadata)
    result = lru_cache.get("k")
    assert result.is_some
    _, val = result.unwrap()
    assert val.tobytes() == b"world"

def test_lru_get_keys(lru_cache, sample_metadata):
    lru_cache.put("alpha", b"a", sample_metadata)
    lru_cache.put("beta",  b"b", sample_metadata)
    keys = lru_cache.get_keys()
    assert "alpha" in keys
    assert "beta"  in keys

# --- LFU extras ---

def test_lfu_remove_explicit(lfu_cache, sample_metadata):
    lfu_cache.put("k1", b"data", sample_metadata)
    lfu_cache.remove("k1")
    assert lfu_cache.get("k1").is_none
    assert lfu_cache.get_used_storage_capacity() == 0

def test_lfu_storage_capacity(lfu_cache):
    assert lfu_cache.get_total_storage_capacity() == 100

def test_lfu_frequency_counter(lfu_cache, sample_metadata):
    lfu_cache.put("k", b"1234567890", sample_metadata)
    lfu_cache.get("k")
    lfu_cache.get("k")
    # put sets freq=1, each get increments: 1 + 2 gets = 3
    assert lfu_cache.freq_counter["k"] == 3

def test_lfu_get_keys(lfu_cache, sample_metadata):
    lfu_cache.put("x", b"x", sample_metadata)
    lfu_cache.put("y", b"y", sample_metadata)
    keys = lfu_cache.get_keys()
    assert "x" in keys
    assert "y" in keys

# --- CacheFactory ---

def test_cache_factory_unknown_policy_falls_back_to_lru():
    from mictlanx.caching import CacheFactory, LRUCache
    cache = CacheFactory.create("UNKNOWN", capacity_storage=50)
    assert isinstance(cache, LRUCache)