import pytest
from mictlanx.utils.compression import CompressionAlgorithm, CompressionX
# import io # No longer needed

def test_zlib_roundtrip():
    """
    Tests that data can be compressed and then decompressed back
    to its original form without any data loss.
    """
    # 1. Define test data in-memory. 
    #    Using repeating data is good for testing compression.
    #    This avoids any external file dependencies.
    original_data = b"This is some sample data for testing the zlib compression. " * 1000
    algorithm = CompressionAlgorithm.ZLIB
    
    # 2. Compress the data
    compress_result = CompressionX.compress_stream(
        algorithm=algorithm,
        data=original_data,
        # params={"level": 5} # You can add this back if needed
    )
    
    # 3. Assert that compression was successful
    assert compress_result.is_ok, f"Compression failed: {compress_result.unwrap_err()}"
    compressed_data = compress_result.unwrap()
    
    # 4. Add a sanity check: compressed data should be smaller
    print(f"Original size: {len(original_data)}, Compressed size: {len(compressed_data)}")
    assert len(compressed_data) < len(original_data), "Compressed data is not smaller than original"

    # 5. Decompress the data
    decompress_result = CompressionX.decompress_stream(
        algorithm=algorithm,
        data=compressed_data,
        chunk_size="1mb" # Kept your original chunk size
    )
    
    # 6. Assert that decompression was successful
    assert decompress_result.is_ok, f"Decompression failed: {decompress_result.unwrap_err()}"
    decompressed_data = decompress_result.unwrap()
    
    # 7. The most important assert: check for data integrity
    assert original_data == decompressed_data, "Decompressed data does not match the original data"