import os
from mictlanx.v4.interfaces.index import ProcessingStructure,Function,Resources
from mictlanx.v4.summoner.summoner import Summoner
# from mictlanx.v3.services.summoner import Summoner,SummonContainerPayload,SummonContainerResponse
from mictlanx.utils.index import Utils as IndexUtils
from option import Option,Some,NONE
from mictlanx.v4.tlaloc.contextual_lang import AvailabilityPolicyMetaobject
from typing import Dict,List,Generator,Tuple
from collections import namedtuple
import numpy as np
from mictlanx.v4.client import Client
from itertools import chain
import time as T

AvailableResourceBase = namedtuple("AvailableResource","ar_id protocol ip_addr port")
AvailableResourceIdBase = namedtuple("AvailableResourceId","cluster_id node_id")
class AvailableResourceId(AvailableResourceIdBase):
    def __str__(self):
        return "{}.{}".format(self.cluster_id,self.node_id)

class AvailableResource(AvailableResourceBase):
    @staticmethod
    def create(ip_addr:str, port:int= -1,protocol:str="http")->"AvailableResource":
        return AvailableResource(protocol=protocol,ip_addr=ip_addr,port=port)
    # def 
# class AvailableResource()




class Tlaloc(object):
    def __init__(self,
                 ip_addr:str,
                 port:int=15000,
                 protocol:str="http",
                 api_version:int = 3,
                 mode:str="docker"
                #  client = 
    ):
        self.mode= mode
        self.summoner = Summoner(
            ip_addr= ip_addr,
            port=port,
            protocol=protocol,
            api_version=Some(api_version),
        )
        self.available_respurces:Dict[str, List[AvailableResource]] = {}
        self.used_ports:List[int] = []
        self.current_ars_by_id:List[AvailableResourceId] = []
        # self.client = 
    def __destroy_nodes(self,ap_available_resources_ids:List[AvailableResourceId]):
        # _current_ar_ids = list(map(lambda x: x[1], amp))
        to_delete= []
        for ar_id in self.current_ars_by_id:
            if not ar_id in ap_available_resources_ids:
                del_res = self.summoner.delete_container(container_id=ar_id,mode=self.mode)
                current_cluster_ars = self.available_respurces.setdefault(ar_id.cluster_id,[])
                self.available_respurces[ar_id.cluster_id] = list(filter(lambda x : not x.ar_id == ar_id,current_cluster_ars) )
                to_delete.append(ar_id)
        self.current_ars_by_id = list(set(self.current_ars_by_id).difference(set(to_delete)))
                
        print("MAP",self.available_respurces)
        print("CUrRENT_ARIDS", self.current_ars_by_id)

    def __get_cluster_id_ar_id(self,amp:AvailabilityPolicyMetaobject)->Generator[AvailableResourceId,List[AvailableResourceId],None]:
        for (cluster_id, nodes) in amp.avaialable_resources.items():
            for n in nodes:
                yield AvailableResourceId(cluster_id=cluster_id,node_id=n)
                # yield (cluster_id, n,"{}.{}".format(cluster_id,n))
        return []
    def build(self,apm: AvailabilityPolicyMetaobject):
        ids =  list(self.__get_cluster_id_ar_id(amp=apm))
        self.__destroy_nodes(ap_available_resources_ids=ids)
        for ar_id in ids:
            # print("DEPLOY",cluster_id,combined_id)
            if ar_id in self.current_ars_by_id:
                print(ar_id,"Already deployed...")
                continue
            cluster_id = ar_id.cluster_id
            # node_id = ar_id.node_id
            combined_id = str(ar_id)
            port = np.random.randint(low=30000, high=60000)
            while port in self.used_ports:
                port = np.random.randint(low=30000, high=60000)

            ar = AvailableResource(ar_id=ar_id,protocol="http", ip_addr="localhost",port=port)
            _ = self.available_respurces.setdefault(cluster_id,[])
            current_peers = list(map(lambda x: "{}:{}".format(x.ar_id,x.port) ,self.available_respurces.get(cluster_id,[])))
            # print(ar.ar_id, current_peers)
            result = self.summoner.summon_peer(container_id=combined_id,port=port,selected_node=cluster_id,peers=current_peers, labels={
                "cluster_id":cluster_id,
                "tlaloc.version":apm.version,
                # "node_id":
            },mode=self.mode)
            if result.is_ok:
                self.available_respurces[cluster_id].append(ar)
                self.used_ports.append(port)
                self.current_ars_by_id.append(ar_id)
        
        _ps = chain.from_iterable(self.available_respurces.values())
        
        peers = list(IndexUtils.peers_from_str_v2(" ".join(map(lambda x: "{}:{}:{}".format(x.ar_id, x.ip_addr, x.port ) , _ps  ))))
        client = Client(
            client_id    = "tlaloc-client-0",
            # 
            peers        = peers,
            # 
            debug        = False,
            # 
            daemon       = True, 
            # 
            max_workers  = 2,
            show_metrics=False,
            # 
            lb_algorithm = "2CHOICES_UF",
            check_peers_availability_interval="90s",
            max_retries=15,
            disable_log=True
        )
        for what_combined_key in apm.what:
            wh_splitted = what_combined_key.split(".")
            whn = len(wh_splitted)
            if whn  == 2:
                (bucket_id,data_id) = wh_splitted
                # print("DATA",bucket_id,data_id)
                for where_peer in apm.where:
                    peer = client.get_peer_by_id(peer_id=where_peer)
                    get_res = client.get_metadata(bucket_id=bucket_id,key=what_combined_key,peer=Some(peer)).result()
                    print("REPLICATE DATA IN",where_peer,get_res)
            elif whn == 1:
                for where_peer in apm.where:
                    print("REPLICATE BUCKET IN",where_peer)
            else:
                print("ERROR")
        print("_"*20)
        client.shutdown()
        print("*"*20)
        # T.sleep(20)
        # client.get_bucket_metadata(bucket_id=)
    def deploy(self,ps:ProcessingStructure):
        completed = []
        for (function_id, next_functions) in ps.functions_order.items():
            if function_id in completed:
                continue
            print("DEPLOY",function_id,"NEXT_FUNCTIONS",next_functions)
            for next_function_id in next_functions:
                print("INEER_DEPLOY",next_function_id)
            completed.extend([function_id,*next_functions])
            print("_"*20)
        print(completed)
    

if __name__ =="__main__":
    tlaloc = Tlaloc(protocol="http",ip_addr="localhost",port=15000)
    ap_str = """
        tlaloc: v1
        available-resources:
            pool-0:
                - peer-0
                - peer-1
                - peer-2
                - peer-3
                - peer-4
            pool-1:
                - peer-0
        who: pool-0.peer-0
        what:
            - bucket-0.bac9b6c65bb832e7a23f936f8b1fdd00051913fc0c483cf6a6f63f89e6588b80
        where:
            - pool-0.peer-1
            - pool-0.peer-2
            - pool-0.peer-3
            - pool-0.peer-4
            - pool-1.peer-0
        how: ACTIVE
        when:
            -bucket0:$GET_COUNTER>10
            -bucket1:$ACCESS_FREQUENNCY>60.6%
    """
    tlaloc.build(apm=AvailabilityPolicyMetaobject.from_str(ap_str=ap_str))
    
    T.sleep(100)
    # ap_str = """
    #     tlaloc: v1
    #     available-resources:
    #         pool-0:
    #             - peer-0
    #             - peer-2
    #     who: pool-0.peer-0
    #     what:
    #         - bucket-1.c3f8c3482bd60da5cb8348e4701ef6d43f4dcb56fbe54ee1f2470526f082a80d
    #     where:
    #         - pool-0.peer-2
    #     how: ACTIVE
    #     when:
    #         -bucket0: $GET_COUNTER>10
    #         -bucket1: $ACCESS_FREQUENNCY>60.6%
    # """
    # tlaloc.build(apm=AvailabilityPolicyMetaobject.from_str(ap_str=ap_str))
    # T.sleep(100)
    
    # ps = ProcessingStructure(
    #     key="ps-0",
    #     functions=[
    #         Function(
    #             key="f1",
    #             image="nachocode/xolo:aes-enc",
    #             resources=Resources(cpu=1,memory="1GB"),
    #             bucket_id="test-bucket-0",
    #             endpoint_id="disys0"
    #         ),
    #         Function(
    #             key="f2",
    #             image="nachocode/utils:lz4",
    #             resources=Resources(cpu=1,memory="1GB"),
    #             bucket_id="test-bucket-0",
    #             endpoint_id="disys1"
    #         ),
    #         Function(
    #             key="f3",
    #             image="nachocode/xolo:aes-dec",
    #             resources=Resources(cpu=1,memory="1GB"),
    #             bucket_id="f2-bucket",
    #             endpoint_id="disys2"
    #         ),
    #     ],
    #     functions_order={
    #         "f1":[],
    #         "f2":["f3"],
    #         "f3":[]
    #     },
    # )
    # res = tlaloc.deploy(ps= ps)