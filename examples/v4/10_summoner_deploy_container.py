import os
import sys
from mictlanx.v3.interfaces.payloads import SummonContainerPayload,ExposedPort,AuthTokenPayload,SignUpPayload,LogoutPayload
from mictlanx.v3.services.summoner import  Summoner
from dotenv import load_dotenv
from option import Some,NONE
from ipaddress import IPv4Network


load_dotenv()
if __name__ =="__main__":
    args            = sys.argv[1:]
    api_version     = Some(int(os.environ.get("MICTLANX_SUMMONER_API_VERSION","3")))
    
    ip_addr = os.environ.get("MICTLANX_SUMMONER_IP_ADDR")
    port = int(os.environ.get("MICTLANX_SUMMONER_PORT"))
    summoner        = Summoner(
        ip_addr     = ip_addr, 
        port        = port, 
        api_version = api_version,
        network= Some(IPv4Network(os.environ.get("MICTLANX_SUMMONER_SUBNET")))
    )
    print(ip_addr,port,api_version)
    print(summoner.base_url)
    ar = 3
    for i in range(ar):
    # i =3
        selected_node = 0
        container_id    = "mictlanx-peer-{}".format(i)
        
        port = 7000+i
        payload         = SummonContainerPayload(
            container_id=container_id,
            image="nachocode/mictlanx:peer",
            hostname    = container_id,
            exposed_ports=[ExposedPort(NONE,port,port,NONE)],
            envs= {
                "USER_ID":"6666",
                "GROUP_ID":"6666",
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
                "WORKERS":"2"
            },
            memory=1000000000,
            cpu_count=1,
            mounts={
                "/mictlanx/{}/data".format(container_id):"/mictlanx/data",
                "/mictlanx/{}/log".format(container_id):"/mictlanx/log", 
                "/mictlanx/{}/local".format(container_id):"/mictlanx/local"
            },
            network_id="mictlanx",
            selected_node=Some(str(selected_node)),
            force=Some(True)
        )
        print(payload.to_dict())
        response        = summoner.summon(
            mode= "swarm",
            payload=payload,
        )
        if response.is_err:
            print(response.unwrap_err())
        else:
            print("RESPONSE",response)