import pytest
from mictlanx.utils.compression import CompressionAlgorithm,CompressionX

import io



@pytest.mark.skip("")
def test_zlib():
    # path = "/source/out.csv"
    # path = "/source/01.pdf"
    path = "/source/f50mb"
    # f    = io.BytesIO(b"HOLAAAAAAAAAAAAAAAAAAAAAA\nASDASDASDASDASDAS-")
    f    = open(path,"rb")
    data = f.read()
    try:
        algorithm = CompressionAlgorithm.ZLIB
        result = CompressionX.compress_stream(
            algorithm = algorithm,
            data      = data,
            # params    = {"level":5}
        )
        print("COMPRESS_RESULT",result)
        if result.is_ok:
            decompress_result = CompressionX.decompress_stream(
                algorithm = algorithm,
                data      = result.unwrap(),
                chunk_size="1mb"
            )
            if decompress_result.is_ok:
                result = decompress_result.unwrap()
            print("DECOMPRES_SREUSLT", result)
    except Exception as e:
        print(e)
    finally:
        f.close()
