import sys
import os
from mictlanx.v3.services.xolo import Xolo
from dotenv import load_dotenv
import requests as R

load_dotenv()
if __name__ =="__main__":
    args      = sys.argv[1:]
    # _________________________
    # 1. Create an instance of Xolo.
    xolo                   = Xolo()
    xolo.key_pair_gen(filename="alice")
    xolo.key_pair_gen(filename="bob")
    alice_key_pair_result  = xolo.load_key_pair(filename="alice").unwrap()
    bob_key_pair_result    = xolo.load_key_pair(filename="bob").unwrap()
    alice_shared_key:bytes = alice_key_pair_result[0].exchange(bob_key_pair_result[1])
    bob_shared_key:bytes   = bob_key_pair_result[0].exchange(alice_key_pair_result[1])
    # shared_key            = bytes.fromhex(secret)
    names = ["ALICE","BOB"]
    for index,shared_key in enumerate([alice_shared_key,bob_shared_key]):
        print("{}_SHARED_KEY".format(names[index]), shared_key)
        encrypted_data_result = xolo.encrypt_aes(key=shared_key,data=b"Hello world")
        print("{}_ENCRYPTED_DATA".format(names[index]),encrypted_data_result)
        if(encrypted_data_result.is_ok):
            plaintext      = xolo.decrypt_aes(key=shared_key, data=encrypted_data_result.unwrap() )
            print("{}_DECRYPTED_DATA".format(names[index]),plaintext)
        print("_"*10)