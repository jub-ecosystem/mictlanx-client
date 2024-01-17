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

class Utils:
    SECRET_PATH = os.environ.get("XOLO_SECRET_PATH","/mictlanx/.secrets")

    
    @staticmethod
    def pbkdf2(password:str,key_length:int=32, iterations:int = 1000,salt_length:int = 16)->str:
        _pass      = password.encode("utf-8")
        salt       = os.urandom(salt_length)
        key        = H.pbkdf2_hmac('sha256', _pass, salt, iterations, key_length)
        # H.pbkdf2_hmac
        return "pbkdf2$l={},sl={},i={}${}/{}".format(key_length,salt_length,iterations,salt.hex(),key.hex())

    @staticmethod
    def check_password_hash(password:str, password_hash:str):
        (version,params,value) = password_hash.split("$")
        (key_length_var, salt_length_var, iterations_var) = params.split(",")
        (_, key_length)  = key_length_var.split("=")
        (_, salt_length) = salt_length_var.split("=")
        (_, iterations)  = iterations_var.split("=")
        (salt,_password_hash) = value.split("/")
        
        # print(version,params,salt,_password_hash)
        # x = bytes.fromhex(password)
        # print(x)
        _key        = H.pbkdf2_hmac('sha256',password.encode("utf-8"), bytes.fromhex(salt), int(iterations), int(key_length)).hex()
        return _key == _password_hash
        # print("LOCAL_KEY",_key.hex())



    @staticmethod
    def sha256(value:bytes)->str:
        h = H.sha256()
        h.update(value)
        return h.hexdigest()
    
    @staticmethod
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

    @staticmethod
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

    @staticmethod
    def load_private_key(filename:str)->Result[Type[X25519PrivateKey],Exception]:
        try:
            private_path = "{}/{}".format(Utils.SECRET_PATH,filename)
            with open(private_path,"rb")  as f:
                x = f.read()
                private_key = serialization.load_pem_private_key(x,password=None)
            return Ok(private_key)
        except Exception as e:
            return Err(e)

    @staticmethod
    def load_public_key(filename:str)->Result[Type[X25519PublicKey],Exception]:
        try:
            public_path  = "{}/{}.pub".format(Utils.SECRET_PATH,filename)
            with open(public_path,"rb")  as f:
                public_key = serialization.load_pem_public_key(f.read())
            return Ok(public_key)
        except Exception as e:
            return Err(e)
        
    @staticmethod
    def load_key_pair(filename:str)->Result[Tuple[Type[X25519PrivateKey],Type[X25519PublicKey]],Exception]:
        try:
            private_key_result = Utils.load_private_key(filename=filename)
            public_key_result = Utils.load_public_key(filename=filename)
            return private_key_result.flatmap(lambda private_key: public_key_result.flatmap(lambda public_key: Ok((private_key,public_key))))
        except Exception as e:
            return Err(e)
            
    @staticmethod
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
        
    @staticmethod
    def decrypt_aes(key:bytes=None,data:bytes=None,header:Option[bytes] =NONE)->Result[bytes,Exception]:
        # iterations = 1000
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
        # iterations = 1000
if __name__ =="__main__":
    x = Utils.pbkdf2(password="xxx",key_length=32,iterations=1000, salt_length=16)
    y = Utils.check_password_hash(password="xx",password_hash=x )
    print(x,y)
    # pass = Utils.