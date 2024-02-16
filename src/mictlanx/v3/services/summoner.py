
import requests as R
from mictlanx.v3.interfaces.service import Service
from mictlanx.v3.interfaces.payloads import SummonContainerPayload,ExposedPort
from mictlanx.v3.interfaces.responses import SummonContainerResponse
from mictlanx.v3.interfaces.errors import ApiError,ServerInternalError
from option import Result,Ok,Err,Some,Option,NONE
from ipaddress import IPv4Network
from typing import Tuple,List,Dict
import numpy as np


class Summoner(Service):
    def __init__(self,ip_addr:str, port:int,protocol:str="http",api_version: Option[int] = NONE,network:Option[IPv4Network]=NONE):
        super(Summoner,self).__init__(ip_addr=ip_addr, port = port, api_version=api_version,protocol=protocol)
        self.summon_container_url                = "{}/{}".format(self.base_url,"containers")
        self.summon_service_url                = "{}/{}".format(self.base_url,"services")
        self.delete_container_url = lambda container_id: "{}/containers/{}".format(self.base_url,container_id)
        self.delete_service_url = lambda container_id: "{}/services/{}".format(self.base_url,container_id)
        self.network:IPv4Network = network.unwrap_or(IPv4Network("10.0.0.0/25"))
        self.reserved_ip_addrs = []
        self.reserved_ports =[]
        # self.complete_operation_url = lambda node_id, operation_type, operation_id: "{}/{}/{}/{}/{}".format(self.base_url,"operations",node_id,operation_type,operation_id)


    
    def __get_available_ip_addr(self,payload:SummonContainerPayload) -> Option[Tuple[str,int]]: 
        port = payload.exposed_ports[0].host_port
        if (payload.ip_addr == payload.container_id):
            return Some((payload.container_id,port))
        elif(payload.ip_addr == "0.0.0.0"):
             return Some((payload.ip_addr, port))
        else :
            for ip_addr in self.network.hosts(): 
                if not ip_addr in self.reserved_ip_addrs:
                    port_predicate = port in self.reserved_ip_addrs
                    while port_predicate:
                        port +=1
                    return Some((ip_addr,port))
                else:continue

    

    def delete_container(self,
                         container_id:str,
                         app_id:Option[str]=NONE,
                         client_id:Option[str] = NONE,
                         authorization:Option[str]= NONE,
                         secret: Option[str]=NONE,
                         mode:str ="docker"
    )->Result[Tuple[()],ApiError] : 
        url = self.delete_service_url(container_id) if not mode =="docker" else self.delete_container_url(container_id)
        headers = {
            "Application-Id": app_id.unwrap_or("APP_ID"),
            "Client-Id":client_id.unwrap_or("CLIENT_ID"),
            "Authorization":authorization.unwrap_or("AUTHORIZATION"),
            "Secret":secret.unwrap_or("SECRET")
        }
        try:
            response = R.delete(url,headers=headers) 
            response.raise_for_status()
            return Ok(())
        except R.RequestException as e:
                response:R.Response = e.response
                return Err(ServerInternalError(message = response.headers.get("Error-Message"), metadata = response.headers))
        

    def summon_peer(self,container_id:str,port:int=-1,selected_node:str="0",mode:str="docker",peers:List[str]=[],labels:Dict[str,str]={})->Result[SummonContainerResponse,ApiError]:
        port = np.random.randint(low=2000, high=60000) if port <= 1024 else port

        payload         = SummonContainerPayload(
            container_id=container_id,
            image="nachocode/mictlanx:peer",
            hostname    = container_id,
            exposed_ports=[ExposedPort(NONE,port,port,NONE)],
            envs= {
                "USER_ID":"1001",
                "GROUP_ID":"1002",
                "BIN_NAME":"peer",
                "NODE_ID":container_id,
                "NODE_PORT":str(port),
                "IP_ADDRESS":container_id,
                "SERVER_IP_ADDR":"0.0.0.0",
                "NODE_DISK_CAPACITY":"10000000000",
                "NODE_MEMORY_CAPACITY":"1000000000",
                "BASE_PATH":"/mictlanx",
                "LOCAL_PATH":"/mictlanx/local",
                "DATA_PATH":"/mictlanx/data",
                "LOG_PATH":"/mictlanx/log",
                "MIN_INTERVAL_TIME":"15",
                "MAX_INTERVAL_TIME":"60",
                "WORKERS":"2",
                "PEERS":" ".join(peers).strip()
            },
            memory=1000000000,
            cpu_count=1,
            mounts={
                "{}-data".format(container_id):"/mictlanx/data",
                "{}-data".format(container_id):"/mictlanx/data",
                # "/mictlanx/{}/log".format(container_id):"/mictlanx/log", 
                # "{}".format(container_id):"/mictlanx/local"
                # "/mictlanx/{}/data".format(container_id):"/mictlanx/data",
                # "/mictlanx/{}/log".format(container_id):"/mictlanx/log", 
                # "/mictlanx/{}/local".format(container_id):"/mictlanx/local"
            },
            network_id="mictlanx",
            selected_node=Some(str(selected_node)),
            force=Some(True),
            labels=labels
        )
        return self.summon(payload=payload, mode=mode)
    def summon(self,
               payload:SummonContainerPayload,
               mode:str= "docker",
               app_id:Option[str]=NONE,
               client_id:Option[str] = NONE, 
               authorization:Option[str]= NONE , 
               secret: Option[str]=NONE, )->Result[SummonContainerResponse,ApiError]:
        try:
            # y = payload.ip_addr
            x  =  self.__get_available_ip_addr(payload=payload)
            if(x.is_none):
                return Err(ServerInternalError())
            (ip_addr, port) = x.unwrap()
            
            # port = payload.exposed_ports[0].host_port
            headers = {
                "Application-Id": app_id.unwrap_or("APP_ID"),
                "Client-Id":client_id.unwrap_or("CLIENT_ID"),
                "Authorization":authorization.unwrap_or("AUTHORIZATION"),
                "Secret":secret.unwrap_or("SECRET")
            }

            # if (payload.ip_addr == payload.container_id):
            
            # payload.envs["NODE_HOST"] = str(ip_addr)
            payload.envs["NODE_ID"] = str(payload.container_id) 
            payload.envs["NODE_IP_ADDR"] = str(ip_addr)
            payload.envs["NODE_PORT"] = str(port)
            # print(payload.to_dict())
            url =self.summon_container_url if mode == "docker" else self.summon_service_url
            response = R.post(
                url,
                json= payload.to_dict(),
                headers=headers
            )
            # print("RM_RESPONSE",response)
            self.reserved_ip_addrs.append(ip_addr)
            self.reserved_ports.append(port)
            response.raise_for_status()
            response_json = response.json()
            return Ok(SummonContainerResponse(**response_json))
        except R.RequestException as e:
                response:R.Response = e.response
                return Err(ServerInternalError(message = response.headers.get("Error-Message"), metadata = response.headers))

