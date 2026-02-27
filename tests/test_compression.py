import pytest
import os
import io
from mictlanx.utils.compression import CompressionX, CompressionAlgorithm, LZ4_AVAILABLE

# --- Fixtures ---

@pytest.fixture
def sample_data():
    """Returns a repetitive string that compresses well."""
    return b"MictlanX" * 1000

@pytest.fixture
def temp_files(tmp_path):
    """Provides paths for input, compressed, and decompressed files."""
    return {
        "input": tmp_path / "input.bin",
        "compressed": tmp_path / "compressed.bin",
        "output": tmp_path / "output.bin"
    }

# --- Tests for Stream Operations ---

@pytest.mark.parametrize("algo", [
    CompressionAlgorithm.ZLIB,
    CompressionAlgorithm.GZIP,
    pytest.param(CompressionAlgorithm.LZ4, marks=pytest.mark.skipif(not LZ4_AVAILABLE, reason="lz4 not installed"))
])
def test_stream_compression_roundtrip(algo, sample_data):
    """Tests if data survives a compress/decompress cycle via streams."""
    # Compress
    res_comp = CompressionX.compress_stream(algo, sample_data)
    assert res_comp.is_ok
    compressed = res_comp.unwrap()

    # Decompress
    res_decomp = CompressionX.decompress_stream(algo, compressed)
    assert res_decomp.is_ok
    assert res_decomp.unwrap() == sample_data

@pytest.mark.parametrize("algo", [
    CompressionAlgorithm.ZLIB,
    CompressionAlgorithm.GZIP,
    pytest.param(CompressionAlgorithm.LZ4, marks=pytest.mark.skipif(not LZ4_AVAILABLE, reason="lz4 not installed"))
])
def test_generator_decompression(algo, sample_data):
    """Verifies that generator-based decompression yields correct chunks."""
    compressed = CompressionX.compress_stream(algo, sample_data).unwrap()
    
    res_gen = CompressionX.decompress_stream_gen(algo, compressed, chunk_size="512B")
    assert res_gen.is_ok
    
    reconstructed = b"".join(list(res_gen.unwrap()))
    assert reconstructed == sample_data

# --- Tests for File Operations ---

def test_zlib_file_operations(temp_files, sample_data):
    """Verifies ZLIB file compression and decompression."""
    temp_files["input"].write_bytes(sample_data)
    
    CompressionX.compress_zlib(str(temp_files["input"]), str(temp_files["compressed"]))
    assert temp_files["compressed"].exists()
    assert temp_files["compressed"].stat().st_size < len(sample_data)

    CompressionX.decompress_zlib(str(temp_files["compressed"]), str(temp_files["output"]))
    assert temp_files["output"].read_bytes() == sample_data

def test_gzip_file_operations(temp_files, sample_data):
    """Verifies GZIP file compression and decompression."""
    temp_files["input"].write_bytes(sample_data)
    
    CompressionX.compress_gzip(str(temp_files["input"]), str(temp_files["compressed"]))
    CompressionX.decompress_gzip(str(temp_files["compressed"]), str(temp_files["output"]))
    
    assert temp_files["output"].read_bytes() == sample_data

@pytest.mark.skipif(not LZ4_AVAILABLE, reason="lz4 not installed")
def test_lz4_file_operations(temp_files, sample_data):
    """Verifies LZ4 file compression and decompression."""
    temp_files["input"].write_bytes(sample_data)
    
    CompressionX.compress_lz4(str(temp_files["input"]), str(temp_files["compressed"]))
    CompressionX.decompress_lz4(str(temp_files["compressed"]), str(temp_files["output"]))

    out_data = temp_files["output"].read_bytes()
    input_data = temp_files["input"].read_bytes()
    assert out_data == input_data
# --- Error Handling Tests ---

def test_invalid_algorithm_params():
    """Tests that the Result pattern catches exceptions."""
    # Passing an integer instead of bytes to trigger an error
    result = CompressionX.compress_stream(CompressionAlgorithm.ZLIB, 12345)
    assert result.is_err
    assert isinstance(result.unwrap_err(), Exception)
