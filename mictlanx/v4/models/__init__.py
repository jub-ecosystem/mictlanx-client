import mictlanx.interfaces as InterfaceX
from typing import List, Dict,Iterator
import humanfriendly as HF
class Ball:
    def __init__(self,bucket_id:str,chunks:List[InterfaceX.Metadata]=[],ball_id:str="",checksum:str="" , bucket_relative_path:str="",fullname:str=""):
        self.bucket_id = bucket_id
        self.ball_id  = ball_id
        self.checksum = checksum
        self.size     = 0
        self.chunks = chunks.copy()
        self.bucket_relative_path = bucket_relative_path
        self.full_path = ""
        self.extension = ""
        self.filename = ""
        self.updated_at = -1
        self.fullname= fullname
    def __str__(self):
        return f"Ball(id={self.ball_id}, size = {self.size})"
    def len_chunks(self):
        return len(self.chunks)
    def add_chunk(self, chunk:InterfaceX.Metadata):
        exists = next(filter(lambda x: x.key == chunk.key and chunk.checksum ==x.checksum, self.chunks),-1)
        # print(f"{self.ball_id} {exists}")
        if exists == -1:
            self.chunks.append(chunk)
        
    def build(self):
        if len(self.chunks) >0:
            c = self.chunks[0] 
            self.checksum = c.tags.get("full_checksum","")
            self.ball_id  = c.ball_id
            self.bucket_relative_path = c.tags.get("bucket_relative_path","")
            self.fullname = c.tags.get("fullname","")
            self.full_path = c.tags.get("full_path","")
            self.extension = c.tags.get("extension","")
            self.filename = c.tags.get("filename","")
            # self.updated_at
        self.size = 0
        sum_updated_at = 0
        for c in self.chunks:
            self.size += c.size
            sum_updated_at += int(c.tags.get("updated_at",0))
        self.updated_at = int(sum_updated_at / len(self.chunks))
        

    def merge(self, other: 'Ball'):
        existing_ids = {c.checksum for c in self.chunks}
        # print(self.ball_id,existing_ids)
        for chunk in other.chunks:
            if chunk.checksum not in existing_ids:
                self.chunks.append(chunk)

class Bucket:
    def __init__(self,bucket_id:str,balls:Dict[str, Ball]):
        self.bucket_id = bucket_id
        self.balls=balls.copy()
    def size_bytes(self)->int:
        size = 0
        for b in self:
            size += b.size
        return size
    def size(self)->str:
        size = self.size_bytes()
        return HF.format_size(size)
            
    def __len__(self)->int:
        return len(self.balls)
    def __iter__(self) -> Iterator['Ball']:
        return iter(self.balls.values() )

    