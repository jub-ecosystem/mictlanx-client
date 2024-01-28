import os
import sys
from mictlanx.v3.interfaces.payloads import SummonContainerPayload,ExposedPort,AuthTokenPayload,SignUpPayload,LogoutPayload
from mictlanx.v3.services.xolo import Xolo
from mictlanx.v3.services.summoner import  Summoner
from dotenv import load_dotenv
from option import Some,NONE
import requests as R
from ipaddress import IPv4Network
from option import Ok


load_dotenv()
if __name__ =="__main__":
    args            = sys.argv[1:]
    api_version     = Some(int(os.environ.get("MICTLANX_API_VERSION","3")))
    
    summoner        = Summoner(
        ip_addr     = os.environ.get("MICTLANX_SUMMONER_IP_ADDR"), 
        port        = int(os.environ.get("MICTLANX_SUMMONER_PORT")), 
        api_version = api_version,
        network= Some(IPv4Network(os.environ.get("MICTLANX_SUMMONER_SUBNET")))
    )
    container_id    = "partition-db-0"
    payload         = SummonContainerPayload(
        container_id=container_id,
        image="moringas/partition-db",
        hostname    = container_id,
        exposed_ports=[ExposedPort(NONE,6000,6000,NONE)],
        envs= {
            "SOURCE_PATH":"/source",
            "SINK_PATH":"/sink", 
            "LOG_PATH":"/log",
        },
        memory=1000000000,
        cpu_count=1,
        mounts={
            "/test/{}/log".format(container_id):"/log",
            "/test/{}/sink".format(container_id):"/sink",
            "/test/{}/source".format(container_id):"/source"   
        },
        network_id="mictlanx",
        selected_node=Some("0")
    )
    response        = summoner.summon(
        mode= "swarm",
        payload=payload,
    )

    