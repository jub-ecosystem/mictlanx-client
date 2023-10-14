import hashlib as H
from Crypto.Cipher import AES
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey,X25519PublicKey
from cryptography.hazmat.primitives.asymmetric.types import PRIVATE_KEY_TYPES,PUBLIC_KEY_TYPES
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.primitives import serialization
from typing import Tuple,Type
from option import Result,Ok,Err,Option,Some,NONE
import os

class Utils(object):
    SECRET_PATH = os.environ.get("XOLO_SECRET_PATH","/mictlanx/.secrets")

    def sha256(value:bytes)->str:
        h = H.sha256()
        h.update(value)
        return h.hexdigest()
    def sha256_file(path:str)->Tuple[str,int]:
        h = H.sha256()
        size = 0
        with open(path,"rb") as f:
            while True:
                data = f.read()
                if not data:
                    return (h.hexdigest(),size)
                size+= len(data)
                h.update(data)

    def  key_pair_gen(filename:str):
        os.makedirs(Utils.secret_path, exist_ok=True)
        private_key = X25519PrivateKey.generate()
        pub_key     = private_key.public_key()
        private_bytes = private_key.private_bytes(
            encoding=serialization.Encoding.PEM, 
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
        public_bytes = pub_key.public_bytes(encoding=serialization.Encoding.PEM,format=serialization.PublicFormat.SubjectPublicKeyInfo)
        private_path = "{}/{}".format(Utils.SECRET_PATH,filename)
        public_path = "{}/{}.pub".format(Utils.SECRET_PATH,filename)
        with open(private_path,"wb") as f:
            f.write(private_bytes)
        with open(public_path,"wb") as f:
            f.write(public_bytes)

    def load_private_key(filename:str)->Result[Type[X25519PrivateKey],Exception]:
        try:
            private_path = "{}/{}".format(Utils.SECRET_PATH,filename)
            with open(private_path,"rb")  as f:
                x = f.read()
                private_key = serialization.load_pem_private_key(x,password=None)
            return Ok(private_key)
        except Exception as e:
            return Err(e)

    def load_public_key(filename:str)->Result[Type[X25519PublicKey],Exception]:
        try:
            public_path  = "{}/{}.pub".format(Utils.SECRET_PATH,filename)
            with open(public_path,"rb")  as f:
                public_key = serialization.load_pem_public_key(f.read())
            return Ok(public_key)
        except Exception as e:
            return Err(e)
        
    def load_key_pair(filename:str)->Result[Tuple[Type[X25519PrivateKey],Type[X25519PublicKey]],Exception]:
        try:
            private_key_result = Utils.load_private_key(filename=filename)
            public_key_result = Utils.load_public_key(filename=filename)
            return private_key_result.flatmap(lambda private_key: public_key_result.flatmap(lambda public_key: Ok((private_key,public_key))))
        except Exception as e:
            return Err(e)
            
    def encrypt_aes(key:bytes=None,data:bytes=None,header:Option[bytes]=NONE)->Result[bytes,Exception]:
        try:
            cipher         = AES.new(key=key,mode=AES.MODE_GCM)
            if(header.is_some):
                cipher.update(header.unwrap())
            ciphertext,tag = cipher.encrypt_and_digest(data)
            nonce          = cipher.nonce
            return Ok(tag+ciphertext+nonce)
        except Exception as e:
            return Err(e)
        
    def decrypt_aes(key:bytes=None,data:bytes=None,header:Option[bytes] =NONE)->Result[bytes,Exception]:
        try:
            tag        = data[:16] 
            ciphertext = data[16:len(data)-16]
            nonce      = data[-16:]
            cipher     =  AES.new(key=key,mode= AES.MODE_GCM,nonce=nonce)
            if(header.is_some):
                cipher.update(header.unwrap())
            return Ok(cipher.decrypt_and_verify(ciphertext=ciphertext,received_mac_tag=tag))
        except Exception as e:
            return Err(e)