import os
import sys
from mictlanx.v4.summoner.summoner import  Summoner
from dotenv import load_dotenv
from option import Some,NONE
import requests as R
from ipaddress import IPv4Network


load_dotenv()
if __name__ =="__main__":
    args      = sys.argv[1:]
    # if(len(args) >= 4  or len(args)==0):
        # raise Exception("Please try to pass a valid file path: python examples/v3/04_xolo.py <APP_ID> <CLIEND_ID> <SECRET>")
    # APP_ID    = args[0]
    # client_id = args[1]
    # secret    = args[2]
    # _________________________
    # 1. Create an instance of Xolo.
    api_version   = Some(int(os.environ.get("MICTLANX_SUMMONER_API_VERSION")))
    summoner = Summoner(
        ip_addr = os.environ.get("MICTLANX_SUMMONER_IP_ADDR"), 
        port = int(os.environ.get("MICTLANX_SUMMONER_PORT")), 
        api_version = api_version,
        network= Some(IPv4Network(os.environ.get("MICTLANX_SUMMONER_SUBNET")))
    )
    # container_id = "scm-1"
    container_ids = ["mictlanx-peer-0","mictlanx-peer-1","mictlanx-peer-2"]
    # ["scm-0"] + list(map(lambda i: "scm-0-scw-{}".format(i),range(5)))

    for container_id in container_ids:
        # response = summoner.delete_container(container_id=container_id, client_id=NONE,app_id=NONE, authorization=NONE, secret=NONE)
        response = summoner.delete_container(container_id=container_id, client_id=NONE,app_id=NONE, authorization=NONE, secret=NONE,mode="swarm")
        if(response.is_err):
            error = response.unwrap_err()
            print("ERROR",error)
        else:
            print("RESPPONSE",response)
        # response = 
        # response = summoner.summon(payload=payload,)