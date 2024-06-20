from abc import ABC
from typing import List,Dict
from option import Option,NONE
import humanfriendly as HF
from collections import namedtuple

class Payload(ABC):
    def __init__(self,*args,**kwargs):
        pass
    def to_dict(self):
        return self.__dict__

class ExposedPort(object):
    def __init__(self,ip_addr:Option[str], host_port:int, container_port:int, protocolo:Option[str]):
        self.ip_addr = ip_addr
        self.host_port = host_port
        self.container_port = container_port 
        self.protocolo = protocolo
    def to_dict(self): 
        x = {}
        if(self.ip_addr.is_some):
            x["ip_addr"] = self.ip_addr.unwrap()
        x["host_port"] = self.host_port
        x["container_port"] = self.container_port 
        if(self.protocolo.is_some):
            x["protocolo"] = self.protocolo.unwrap()
        return x
            
    
MountXBase = namedtuple("MountX","source target mount_type")
mountx_map = {
    "BIND":0,
    "VOLUME":1,
    "TMPFS":2,
    "NPIPE":3,
    "EMPTY":4
}
class MountX(MountXBase):
    def to_dict(self):
        return {
            "source":self.source,
            "target":self.target,
            "mount_type":int(self.mount_type)
        }

class SummonContainerPayload(Payload):
    def __init__(self,
                 container_id:str,
                 image:str,
                #  image_tag:str,
                 hostname:str,
                 exposed_ports:List[ExposedPort],
                 envs:Dict[str,str],
                 memory:int,
                 cpu_count:int,
                 mounts:List[MountX],
                 network_id:str,
                 selected_node:Option[str] = NONE,
                 labels:Dict[str,str ]= {},
                 force:Option[bool]=NONE,
                 ip_addr:Option[str]=NONE,
                 shm_size:Option[str] =NONE
                ):
        self.container_id  = container_id
        self.image         = image
        # self.image_tag     = image_tag 
        self.hostname      = hostname
        self.exposed_ports = exposed_ports
        self.envs          = envs
        self.memory        = memory
        self.cpu_count     = cpu_count
        self.mounts:List[MountX]        = mounts
        self.network_id    = network_id
        self.selected_node = selected_node
        # .unwrap_or("0")
        self.labels        = labels
        self.force         = force.unwrap_or(True)
        self.ip_addr       = ip_addr.unwrap_or(self.container_id)
        self.shm_size = shm_size
    def to_dict(self):
        mountx = [ i.to_dict() for i in self.mounts]
        current_dict = {
            "container_id":self.container_id,
            "image":self.image,
            "hostname":self.hostname,
            "exposed_ports":list(map(lambda x: x.to_dict(),self.exposed_ports)),
            "envs":self.envs,
            "memory":self.memory,
            "cpu_count": self.cpu_count, 
            "mounts":mountx,
            "network_id":self.network_id,
            "labels":{**self.labels,**{"target":"mictlanx"}},
            "force":self.force,
        }
        if self.selected_node.is_some:
            current_dict["selected_node"] = self.selected_node.unwrap()
        if self.shm_size.is_some:
            current_dict["shm_size"]=HF.parse_size(self.shm_size.unwrap())
        return current_dict
    
if __name__ == "__main__":
    sc = SummonContainerPayload(
        container_id="scm-0",
        image="secure-clustering:manager",
        hostname = "scm-0",
        exposed_ports=[ExposedPort(NONE,6000,6000,NONE)],
        envs= {
            "NODE_PORT":"6000",
            "NODE_HOST":"0.0.0.0",
            "NODE_PREFIX":"scw-",
            "MAX_WORKERS":"5",
            "DOCKER_IMAGE_NAME":"secure-clustering",
            "DOCKER_IMAGE_TAG":"worker",
            "MICTLANX_SUMMONER_IP_ADDR":"10.0.0.100",
            "MICTLANX_SUMMONER_PORT":"15000",
            "MICTLANX_API_VERSION":"3",
            "DEBUG":"0",
            "RELOAD":"0",
            "LOG_PATH":"/log",
            "SINK_PATH":"/sink", 
            "SOURCE_PATH":"/source",
            "TESTING":"0",
            "MAX_RETRIES":"10",
            "LOAD_BALANCING":"0"
        },
        memory=1000000000,
        cpu_count=1,
        mounts={
            "/log":"/log",
            "/sink":"/sink",
            "/source":"/source"   
        }
    )
    print(sc.to_dict())
