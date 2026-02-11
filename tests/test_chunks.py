import pytest
import os
import numpy as np
import tempfile
from option import Some
from mictlanx.utils.segmentation import Chunk, Chunks

def test_chunks_from_list():
    N            = 100
    n_chunks     = 10
    group_id     = "x"
    chunk_prefix = "x"
    maybe_chs = Chunks.from_list(
        xs           = list(range(N)),
        chunk_prefix = Some(chunk_prefix),
        num_chunks   = n_chunks,
        group_id     = group_id
    )
    assert maybe_chs.is_some
    chs = maybe_chs.unwrap()
    assert len(chs) == 10
    # for c in chs:
        # print(c.to_list())

def test_chunk_from_list():
    c = Chunk.from_list( group_id="x", index=0, xs=[1,2,3], metadata={}, chunk_id=Some("x"))   
    xs = c.to_list()
    assert xs.is_some
    assert xs.unwrap() == [1,2,3]

def test_from_nparray():
    xs = np.ones((10,22,12))
    maybe_chs = Chunks.from_ndarray(
        ndarray=xs,
        group_id="test",
        chunk_prefix=Some("test"),
        num_chunks=5
    )
    assert maybe_chs.is_some

    chs = maybe_chs.unwrap()
    chs.sort(reverse=False)
    for i,c in enumerate(chs):
        assert c.index == i


# --- Fixtures ---

@pytest.fixture
def sample_data():
    return b"0123456789"

@pytest.fixture
def sample_ndarray():
    return np.array([[1, 2], [3, 4], [5, 6]], dtype="int32")

@pytest.fixture
def sample_list():
    return [1, "two", {"three": 3}]

# --- Chunk Tests ---

def test_chunk_basic_init(sample_data):
    chunk = Chunk(group_id="g1", index=0, data=sample_data)
    assert chunk.size == 10
    assert chunk.group_id == "g1"
    assert chunk.index == 0
    assert isinstance(chunk.checksum, str)
    assert chunk.chunk_id == chunk.checksum

def test_chunk_to_list(sample_list):
    chunk = Chunk.from_list("g1", 0, sample_list)
    recovered = chunk.to_list()
    assert recovered.is_some
    assert recovered.unwrap() == sample_list

def test_chunk_to_ndarray(sample_ndarray):
    chunk = Chunk.from_ndarray("g1", 0, sample_ndarray)
    recovered = chunk.to_ndarray()
    assert recovered.is_some
    np.testing.assert_array_equal(recovered.unwrap(), sample_ndarray)

def test_chunk_generators(sample_data):
    chunk = Chunk(group_id="g1", index=0, data=sample_data)
    # Test synchronous generator (2 bytes at a time)
    gen = list(chunk.to_generator(chunk_size="2b"))
    assert len(gen) == 5
    assert b"".join(gen) == sample_data

@pytest.mark.asyncio
async def test_chunk_async_generator(sample_data):
    chunk = Chunk(group_id="g1", index=0, data=sample_data)
    parts = []
    async for part in chunk.to_async_generator(chunk_size="5b"):
        parts.append(part)
    assert len(parts) == 2
    assert b"".join(parts) == sample_data

# --- Chunks Factory & Iteration Tests ---

def test_chunks_from_list(sample_list):
    # Split list of 3 items into 3 chunks
    chunks_opt = Chunks.from_list(sample_list, "g1", num_chunks=3)
    assert chunks_opt.is_some
    chunks = chunks_opt.unwrap()
    assert len(chunks) == 3
    
    # Test iterator protocol
    indices = [c.index for c in chunks]
    assert indices == [0, 1, 2]

def test_chunks_from_ndarray(sample_ndarray):
    # sample_ndarray has 3 rows. Split into 2 chunks.
    chunks_opt = Chunks.from_ndarray(sample_ndarray, "g1", num_chunks=2)
    assert chunks_opt.is_some
    chunks = chunks_opt.unwrap()
    
    # Reconstruct
    reconstructed_opt = chunks.to_ndarray()
    print("reconstructed_opt", reconstructed_opt)
    assert reconstructed_opt.is_some
    arr, meta = reconstructed_opt.unwrap()
    np.testing.assert_array_equal(arr, sample_ndarray)
    assert meta.size == sample_ndarray.nbytes

def test_chunks_from_file(tmp_path):
    p = tmp_path / "test.bin"
    content = b"abcdefghij" # 10 bytes
    p.write_bytes(content)
    
    # Split into 5 chunks of 2 bytes
    chunks_opt = Chunks.from_file(str(p), "g1", chunk_size=Some(2))
    assert chunks_opt.is_some
    chunks = chunks_opt.unwrap()
    
    assert chunks.to_bytes() == content
    assert len(list(chunks.iter())) == 5

def test_chunks_from_bytes():
    data = b"hello world"
    chunks_opt = Chunks.from_bytes(data, "g1", num_chunks=2)
    assert chunks_opt.is_some
    assert chunks_opt.unwrap().to_bytes() == data

def test_chunks_sorting():
    c1 = Chunk("g", 1, b"b")
    c0 = Chunk("g", 0, b"a")
    chs = Chunks(iter([c1, c0]), 2)
    chs.sort()
    assert chs.chunks[0].index == 0
    
    sorted_iter = list(chs.sorted_by(reverse=True))
    assert sorted_iter[0].index == 1

# --- Edge Cases & Error Handling ---

def test_chunks_invalid_ndarray_conversion():
    # Chunk without shape metadata should return NONE
    chunk = Chunk("g1", 0, b"data")
    chs = Chunks(iter([chunk]), 1)
    assert chs.to_ndarray().is_none

def test_chunk_invalid_list_conversion():
    # Non-pickled data should return NONE
    chunk = Chunk("g1", 0, b"not_a_pickle")
    assert chunk.to_list().is_none

def test_chunks_from_empty_file(tmp_path):
    p = tmp_path / "empty.bin"
    p.write_bytes(b"")
    assert Chunks.from_file(str(p), "g1").is_none

def test_iter_to_chunks_logic():
    # Internal logic test for _iter_to_chunks
    data = [1, 2, 3, 4, 5]
    res = Chunks._iter_to_chunks("g1", data, n=5, chunk_size=Some(2),strict=True)
    print("res", res)
    # Expected: index 0 (1,2), index 1 (3,4), index 2 (5)
    assert len(res) == 3
    assert res[0]['data'] == [1, 2]
    assert res[2]['data'] == [5]



def test_from_generator_stdlib():
    tmp_file_path = None
    try:
        # 1. Setup: Create the temporary file
        size_in_mb = 50
        size_in_bytes = size_in_mb * 1024 * 1024

        # Create a named temp file, delete=False so we can close it and use it
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.seek(size_in_bytes - 1)
            f.write(b'\0')
            tmp_file_path = f.name
        
        # 2. Run the test logic
        maybe_chs = Chunks.from_file(path=tmp_file_path, num_chunks=5, group_id="test")
        
        if maybe_chs.is_none:
            assert False, "Fail chunks creation"
        
        chs = maybe_chs.unwrap()
        gen = chs.to_generator()
        
        maybe_chunks = Chunks.from_generator(
            gen=gen,
            group_id="test",
            chunk_size=Some("10mb"),
            num_chunks=1,
        )
        
        if maybe_chunks.is_none:
            assert False, "From generator failed"
        
        chunks = maybe_chunks.unwrap()
        print("chunks", chunks)
        for c in chunks:
            print(c)
        
        assert True, "Success"

    finally:
        # 3. Teardown: Clean up the file, no matter what
        if tmp_file_path and os.path.exists(tmp_file_path):
            os.remove(tmp_file_path)
