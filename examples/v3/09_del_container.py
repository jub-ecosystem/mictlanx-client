
import os
import sys
from mictlanx.v3.interfaces.payloads import SummonContainerPayload,ExposedPort,AuthTokenPayload,SignUpPayload
from mictlanx.v3.services.xolo import Xolo
from mictlanx.v3.services.summoner import  Summoner
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
    api_version   = Some(int(os.environ.get("MICTLANX_API_VERSION")))
    xolo          = Xolo(ip_addr = os.environ.get("MICTLANX_XOLO_IP_ADDR"), port = int(os.environ.get("MICTLANX_XOLO_PORT")), api_version = api_version )
    APP_ID        = os.environ.get("MICTLANX_APP_ID")
    CLIENT_ID     = os.environ.get("MICTLANX_CLIENT_ID")
    SECRET        = os.environ.get("MICTLANX_SECRET")
    EXPIRES_IN    = os.environ.get("MICTLANX_EXPIRES_IN")
    auth_result = xolo.auth(
        payload=AuthTokenPayload(
            app_id     = APP_ID,
            client_id  = CLIENT_ID,
            secret     = SECRET,
            expires_in = Some(EXPIRES_IN)
        ), 
        # signup_payload= Some( SignUpPayload(app_id=APP_ID,client_id=CLIENT_ID,secret=SECRET,expires_in=Some(EXPIRES_IN)))
    )
    if(auth_result.is_ok):
        auth_response = auth_result.unwrap()
    # 1. Create an instance of Summoner
        # summonter_i
        summoner = Summoner(
            ip_addr = os.environ.get("MICTLANX_SUMMONER_IP_ADDR"), 
            port = int(os.environ.get("MICTLANX_SUMMONER_PORT")), 
            api_version = api_version,
            network= Some(IPv4Network(os.environ.get("MICTLANX_SUMMONER_SUBNET")))
        )
        # container_id = "scm-1"
        print("TOKEN",auth_response.token)
        container_ids = ["scm-0"] + list(map(lambda i: "scm-0-scw-{}".format(i),range(5)))
        for container_id in container_ids:
            response = summoner.delete_container(container_id=container_id, client_id=Some(CLIENT_ID),app_id=Some(APP_ID), authorization=Some(auth_response.token), secret=Some(SECRET))
            if(response.is_err):
                error = response.unwrap_err()
                print("ERROR",error)
            else:
                print("RESPPONSE",response)
        # response = 
        # response = summoner.summon(payload=payload,)