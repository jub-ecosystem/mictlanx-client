import pytest
from mictlanx.utils import Chunks,Utils,Chunk
from option import Some,NONE
import numpy as np


@pytest.mark.skip("")
def test_chunks_from_list():
    maybe_chs = Chunks.from_list(
        xs= list(range(100)),
        chunk_prefix=Some("x"),
        num_chunks=10,
        group_id="x"
    )
    assert maybe_chs.is_some
    chs = maybe_chs.unwrap()
    for c in chs:
        print(c.to_list())

@pytest.mark.skip("")
def test_chunk_from_list():
    c = Chunk.from_list( group_id="x", index=0, xs=[1,2,3], metadata={}, chunk_id=Some("x"))   
    print(c)
    xs = c.to_list()
    print(xs)


@pytest.mark.skip("")
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
    chs.sort(reverse=True)
    for i,c in enumerate(chs):
        assert c.index == i

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
