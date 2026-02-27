import pytest
from mictlanx.utils import Utils
from mictlanx.utils.index import FileInfo
import pytest
from unittest.mock import patch

# --- FileInfo Tests ---

def test_file_info_relative_path():
    info = FileInfo(path="/home/user/data/file.txt", checksum="abc", size=100)
    updated = info.update_path_relative_to("/home/user")
    assert updated.path == "data/file.txt"
    assert updated.checksum == "abc"
    assert updated.size == 100

# --- Utils String & Path Tests ---

def test_camel_to_snake():
    assert Utils.camel_to_snake("MyTestClass") == "MY_TEST_CLASS"
    assert Utils.camel_to_snake("HTTPServer") == "HTTP_SERVER"

def test_split_path_file():
    parent, stem, ext = Utils.split_path("/path/to/my_file.txt", is_file=True)
    assert stem == "my_file"
    assert ext == "txt"
    assert "path/to" in parent

def test_split_path_dir():
    parent, stem, ext = Utils.split_path("/path/to/dir", is_file=False)
    assert stem == ""
    assert ext == ""
    assert parent.endswith("dir")

def test_sanitize_str():
    assert Utils.sanitize_str("My@Invalid#String!") == "MyInvalidString"
    assert Utils.sanitize_str("valid-name_123") == "valid-name_123"
    assert Utils.sanitize_str("---multiple--dashes---") == "multiple-dashes"
    assert Utils.sanitize_str("!@#$") == ""

# --- Utils Iterator & Logic Tests ---

def test_get_or_default():
    items = [10, 20, 30]
    assert Utils.get_or_default(items, 0).unwrap() == 10
    assert Utils.get_or_default(items, 2).unwrap() == 30
    assert Utils.get_or_default(items, 5, default="none").unwrap() == "none"
    assert Utils.get_or_default([], 0).is_none

def test_calculate_disk_uf():
    # total 100, used 20, adding 30 -> 50/100 = 0.5
    assert Utils.calculate_disk_uf(100, 20, 30) == 0.5
    # total 1000, used 900 -> 0.9
    assert Utils.calculate_disk_uf(1000, 900, 0) == 0.9

# --- Generator & Async Tests ---

def test_to_gen_bytes():
    data = b"abcdefghij" # 10 bytes
    # Split into 3-byte chunks
    gen = list(Utils.to_gen_bytes(data, chunk_size="3B"))
    assert gen == [b"abc", b"def", b"ghi", b"j"]

@pytest.mark.asyncio
async def test_to_async_gen_bytes():
    data = b"12345678"
    parts = []
    async for part in Utils.to_async_gen_bytes(data, chunk_size="4B"):
        parts.append(part)
    assert parts == [b"1234", b"5678"]

def test_file_to_chunks_gen(tmp_path):
    p = tmp_path / "test_file.bin"
    p.write_bytes(b"HELLO_WORLD")
    
    # Read in 5-byte chunks
    chunks = list(Utils.file_to_chunks_gen(str(p), chunk_size="5B"))
    assert chunks == [b"HELLO", b"_WORL", b"D"]

# --- Multi-threaded Generator Test ---

@patch("xolo.utils.utils.Utils.extract_path_sha256_size")
def test_get_checksums_and_sizes(mock_extract, tmp_path):
    # Create a fake file structure
    d = tmp_path / "sub"
    d.mkdir()
    f1 = d / "file1.txt"
    f1.write_text("content")
    
    # Mock return value (path, checksum, size)
    mock_extract.return_value = (str(f1), "hash123", 7)
    
    results = list(Utils.get_checksums_and_sizes(str(tmp_path)))
    
    assert len(results) == 1
    assert results[0].checksum == "hash123"
    assert results[0].size == 7
    mock_extract.assert_called_once()