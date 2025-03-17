import pytest
from mictlanx.utils import Chunks,Utils
from option import Some,NONE
import numpy as np

# @pytest.mark.skip("")
def test_from_nparray():
    xs = np.ones((10,22,12))
    maybe_chs = Chunks.from_ndarray(
        ndarray=xs,
        group_id="test",
        chunk_prefix=Some("test"),
        num_chunks=5
    )
    if maybe_chs.is_none:
        assert "Failed"
    chs = maybe_chs.unwrap()
    chs.sort(reverse=True)
    for c in chs:
        print(c)
    print("NUM_CHUNKS", len(chs))

@pytest.mark.skip("")
def test_from_generator():
    maybe_chs = Chunks.from_file(path="/source/f50mb",num_chunks=5,group_id="test")
    if maybe_chs.is_none:
        assert False, "Fail chunks creation"
    chs = maybe_chs.unwrap()
    gen = chs.to_generator()
    maybe_chunks = Chunks.from_generator(
        gen= gen, 
        group_id="test",
        chunk_size=Some("10mb"),
        num_chunks=1,
    )
    if maybe_chunks.is_none:
        assert False, "From generator failed"
    chunks = maybe_chunks.unwrap()
    print("chunks",chunks)
    for c in chunks:
        print(c)
    assert True, "Success"
