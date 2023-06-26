
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
    args      = sys.argv[1:]
    # 1. Create an instance of Xolo.
    api_version   = Some(int(os.environ.get("MICTLANX_API_VERSION")))
    xolo          = Xolo(ip_addr = os.environ.get("MICTLANX_XOLO_IP_ADDR"), port = int(os.environ.get("MICTLANX_XOLO_PORT")), api_version = api_version )
    APP_ID        = "rory_pErpitLtqdA6lJ95OQsbzGiJFgAEHsHF"
    
    # 
    # xolo_credentials = {
    #     "sreyes"
    # }
    CLIENT_ID     = "sreyes"
    SECRET        = os.environ.get("MICTLANX_SECRET")
    EXPIRES_IN    = os.environ.get("MICTLANX_EXPIRES_IN")
    # auth_result = Ok()
    app_id = os.environ.get("MICTLANX_APP_ID")
    client_id = os.environ.get("MICTLANX_CLIENT_ID")
    auth_result = xolo.auth(
        payload=AuthTokenPayload(
            app_id     = app_id,
            client_id  = client_id,
            secret     = SECRET,
            expires_in = Some(EXPIRES_IN)
        ), 
    )
    print(auth_result)
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
        container_id = "rory-manager-0"

        payload = SummonContainerPayload(
            container_id=container_id,
            image="shanelreyes/rory:manager",
            hostname = container_id,
            exposed_ports=[ExposedPort(NONE,6000,6000,NONE)],
            envs= {
                "NODE_PREFIX":"{}-worker-".format(container_id),
                "INIT_WORKERS":"2",
                "MAX_WORKERS":"10",
                "DOCKER_IMAGE_NAME":"shanelreyes/rory",
                "DOCKER_IMAGE_TAG":"worker",
                "WORKER_INIT_PORT":"4000",
                "MICTLANX_SUMMONER_IP_ADDR":"mictlanx-summoner-0",
                "MICTLANX_SUMMONER_PORT":str(summoner.port),
                "MICTLANX_API_VERSION":str(api_version.unwrap_or(3)),
                "MICTLANX_APP_ID":APP_ID,
                "MICTLANX_CLIENT_ID":"sreyes0",
                "MICTLANX_SECRET":SECRET,
                "MICTLANX_PROXY_IP_ADDR":"mictlanx-proxy-0",
                # os.environ.get("MICTLANX_PROXY_IP_ADDR"),
                "MICTLANX_PROXY_PORT":os.environ.get("MICTLANX_PROXY_PORT"), 
                "MICTLANX_XOLO_IP_ADDR":"mictlanx-xolo-0",
                # os.environ.get("MICTLANX_XOLO_IP_ADDR"),
                "MICTLANX_XOLO_PORT":os.environ.get("MICTLANX_XOLO_PORT"),
                "MICTLANX_REPLICA_MANAGER_IP_ADDR":"mictlanx-rm-0",
                # os.environ.get("MICTLANX_REPLICA_MANAGER_IP_ADDR"),
                "MICTLANX_REPLICA_MANAGER_PORT":os.environ.get("MICTLANX_REPLICA_MANAGER_PORT"),
                "MICTLANX_EXPIRES_IN":os.environ.get("MICTLANX_EXPIRES_IN"),
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
            },
            network_id="mictlanx",
            # ip_addr=Some("0.0.0.0")
        )
        # print(sc.to_dict())
        print("TOKEN",auth_response.token)
        # logout_payload = LogoutPayload(app_id=ap)
        response = summoner.summon(payload=payload,client_id=Some(client_id),app_id=Some(app_id), authorization=Some(auth_response.token), secret=Some(SECRET))
        # xolo.logout()
    
        if(response.is_err):
            error = response.unwrap_err()
            print("ERROR",error)
        else:
            print("RESPPONSE",response)