import hashlib as H
from typing import Tuple
class Utils:
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