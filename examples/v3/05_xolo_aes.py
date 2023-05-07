import sys
import os
from mictlanx.v3.services.xolo import Xolo
from dotenv import load_dotenv
import requests as R

load_dotenv()
if __name__ =="__main__":
    args      = sys.argv[1:]
    if(len(args) >= 4  or len(args)==0):
        raise Exception("Please try to pass a valid file path: python examples/v3/05_xolo_aes.py <SECRET>")
    secret    = args[0]
    # _________________________
    # 1. Create an instance of Xolo.
    xolo                  = Xolo()
    shared_key            = bytes.fromhex(secret)
    encrypted_data_result = xolo.encrypt_aes(key=shared_key,data=b"Hello world")
    print("ENCRYPTED_DATA",encrypted_data_result)
    if(encrypted_data_result.is_ok):
        plaintext      = xolo.decrypt_aes(key=shared_key, data=encrypted_data_result.unwrap() )
        print("DECRYPTED_DATA",plaintext)