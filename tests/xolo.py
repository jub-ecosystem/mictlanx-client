import os
import unittest 
from mictlanx.v3.services.xolo import Xolo
from option import Some,NONE

xolo             = Xolo(ip_addr = os.environ.get("MICTLANX_XOLO_IP_ADDR"), port=int(os.environ.get("MICTLANX_XOLO_PORT",10000)), api_version=Some(3))
class XoloTestSuite(unittest.TestCase):
    def test_decrypt_aes(self):
        secret_key = "e3182e0bd34b401a06b6be65e98c64e33bc98a9542d0033044b35f83251786fb"
        secret_key = bytes.fromhex(secret_key)
        path       = "/mnt/c/Users/isc_s/Downloads/test"
        with open(path,"rb") as f:
            data = f.read()
            result = xolo.decrypt_aes(key=secret_key, data=data)
            print("RESULT",result)
            

if __name__ =="__main__":
    unittest.main()