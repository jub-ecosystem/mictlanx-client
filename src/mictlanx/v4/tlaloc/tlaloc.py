from mictlanx.v4.interfaces.index import ProcessingStructure,Function,Resources
from mictlanx.v3.services.summoner import Summoner,SummonContainerPayload,SummonContainerResponse
from option import Option,Some,NONE

class Tlaloc(object):
    def __init__(self,ip_addr:str, port:int=15000,protocol:str="http"):
        self.summoner = Summoner(
            ip_addr= ip_addr,
            port=port,
            api_version=Some(3),
        )
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
    
    ps = ProcessingStructure(
        key="ps-0",
        functions=[
            Function(
                key="f1",
                image="nachocode/xolo:aes-enc",
                resources=Resources(cpu=1,memory="1GB"),
                bucket_id="test-bucket-0",
                endpoint_id="disys0"
            ),
            Function(
                key="f2",
                image="nachocode/utils:lz4",
                resources=Resources(cpu=1,memory="1GB"),
                bucket_id="test-bucket-0",
                endpoint_id="disys1"
            ),
            Function(
                key="f3",
                image="nachocode/xolo:aes-dec",
                resources=Resources(cpu=1,memory="1GB"),
                bucket_id="f2-bucket",
                endpoint_id="disys2"
            ),
        ],
        functions_order={
            "f1":[],
            "f2":["f3"],
            "f3":[]
        },
    )
    res = tlaloc.deploy(ps= ps)