from typing import List,Dict,Any,Set
from option import Result,Err,Ok
from mictlanx.v4.interfaces.responses import PutMetadataResponse,GetUFSResponse
import requests as R


class DistributionSchema(object):
    def __init__(self):
        self.__schema:Dict[str, BallContext] = {}
    def put(self,key:str,size:int,locations:List[str] = [] ):
        self.__schema.setdefault(key,BallContext(size=size,locations=[] ))
        self.__schema[key].locations.extend(locations)


class PeerStats(object):
    def __init__(self,peer_id:str): 
        self.__peer_id            = peer_id
        self.disk_uf:float        = 0.0
        self.total_disk:int       = 0
        # self.used_disk:int        = 0 
        
        self.put_counter:int      = 0
        self.get_counter:int      = 0 
        self.balls                = set()

        self.get_counter_per_ball:Dict[str,int] = {}
        # self.put_frecuency:float = 0.0
        # self.get_frecuency:float = 0.0

    def put_frequency(self):
        return self.put_counter / self.global_counter()
    
    def get_frequency(self):
        return self.get_counter / self.global_counter()

    def get_frecuency_per_ball(self):
        res = {}
        for key, getcounter in self.get_counter_per_ball.items():
            res[key] = getcounter / self.get_counter
        return res
    
    def put(self,key:str, size:int):
        self.balls.add(key)
        self.put_counter+=1
        if not key in self.balls:
            self.get_counter_per_ball[key] = 0
            self.used_disk+=size

    def get(self, key:str, size:int):
        self.get_counter += 1
        self.balls.add(key)
        if not key in self.get_counter_per_ball:
            self.get_counter_per_ball[key] = 1
        else:
            self.get_counter_per_ball[key] += 1 
    def delete(self,key:str,size:int):
        self.balls.discard(key)
        if self.used_disk >=size:
            self.used_disk-=size
        del self.get_counter_per_ball[key]
    
    def calculate_disk_uf(self,size:int = 0 ):
        return  1 - ((self.total_disk - (self.used_disk + size))/self.total_disk)
    
    def available_disk(self):
        return self.total_disk - self.used_disk

    def global_counter(self):
        return self.put_counter + self.get_counter
    
    def __str__(self):
        return "PeerStats(peer_id={}, total_disk={}, used_disk={}, available_disk={}, puts={}, gets={}, globals={})".format(self.__peer_id,self.total_disk,self.used_disk, self.available_disk(),self.put_counter,self.get_counter,self.global_counter())

    # ef 
class Peer(object):
    def __init__(self, peer_id:str, ip_addr:str, port:int):
        self.peer_id = peer_id
        self.ip_addr = ip_addr
        self.port = port
    def empty():
        return Peer(peer_id="",ip_addr="",port=-1)
    def get_addr(self)->str :
        return "{}:{}".format(self.ip_addr,self.port)
    def http_url(self):
        return "http://{}:{}".format(self.ip_addr,self.port)
    
    def put_metadata(self, key:str, size:int, checksum:str, tags:Dict[str,str], producer_id:str, content_type:str, ball_id:str, bucket_id:str)->Result[PutMetadataResponse, Exception]:
            try:
                put_metadata_response =R.post("{}/api/v{}/metadata".format(self.http_url(),4),json={
                    "key":key,
                    "size":size,
                    "checksum":checksum,
                    "tags":tags,
                    "producer_id":producer_id,
                    "content_type":content_type,
                    "ball_id":ball_id,
                    "bucket_id":bucket_id
                })
                put_metadata_response.raise_for_status()
                return Ok(PutMetadataResponse(**put_metadata_response.json()))
            except Exception as e:
                return Err(e)
    def put_data(self,task_id:str,key:str, value:bytes, content_type:str) -> Result[Any, Exception]:
        try:
            put_response = R.post(
                "{}/api/v{}/data/{}".format(self.http_url(), 4,task_id),
                files= {
                    "upload":(key,value,content_type)
                },
                )
            put_response.raise_for_status()
            return  Ok(())
        except Exception as e:
            return Err(e)

    def get_ufs(self)->Result[GetUFSResponse, Exception]:
        try:
            response = R.get("{}/api/v4/stats/ufs".format(self.http_url()))
            response.raise_for_status()
            return Ok(GetUFSResponse(**response.json()))
        except Exception as e:
            return Err(e)
    def __str__(self):
        return "Peer(id = {}, ip_addr={}, port={})".format(self.peer_id, self.ip_addr,self.port)


class BallContext(object):
    def __init__(self,size:int, locations:Set[str]):
        self.size = size
        self.locations = locations


if __name__ =="__main__":
    peer = Peer(peer_id="mictlanx-peer-0",ip_addr="localhost",port=7000)
    m = peer.put_metadata( 
        key="test",
        size= 0,
        checksum="CHECKSUM",
        tags= {"EXAMPLE":"BALUE"},
        producer_id= "PRODUCER_I",
        content_type="application/octet-stream",
        ball_id="BALL_DI",
        bucket_id="BUCKE_ID"
    ).unwrap()
    print(m)
    print(
        # peer.put_data(task_id= m.task_id, key=m.key, value=b"HOLAAA" , content_type="application/octet-stream").unwrap_err().response.headers
    )
    # print(peer.get_ufs().unwrap())