import unittest as UT
from multiprocessing import shared_memory
import time as T


class MemoryTesT(UT.TestCase):
    @UT.skip("aa")
    def test_ok(self):
        print("aa")
        shm_a = shared_memory.SharedMemory(create=True, size=100000000,name="test")
        shm_a.buf[:4] = b"Hola"
        # print(shm_a.name)
        T.sleep(100)
        shm_a.close()
        # shm_a.unlink()

    # @UT.skip("aa")
    def test_ok2(self):
        # psm_b16d39dd
        shm_a = shared_memory.SharedMemory(name="test")
        print(shm_a.buf[:4].tobytes())
        T.sleep(100)
        shm_a.close()
        shm_a.unlink()


if __name__ =="__main__":
    UT.main()
