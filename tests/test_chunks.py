import pytest
import os
from mictlanx.utils import Chunks,Chunk
import numpy as np
import tempfile
from option import Some

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
