import os
import time as T
from scipy import stats as S
from mictlanx.v4.client import Client
from mictlanx.v4.interfaces.responses import PutResponse
from mictlanx.utils.index import Utils
from mictlanx.logger.log import Log
from option import Result
from typing import Awaitable,List,Generator,Tuple
from concurrent.futures import as_completed
import json as J
import pandas as pd

# Basic logger from MictlanX utils
L = Log(
    name          = "test",
    create_folder = False,
    to_file       = False,
    error_log     = False
)


def generate_filename_path_from_os_walk(source_folder:str)->Generator[Tuple[str,str],None,None]:
    for (root,_,filenames) in os.walk(source_folder):
        for filename in filenames:
            file_path = "{}/{}".format(source_folder,filename)
            yield filename,file_path


def generate_rows_from_df(df:pd.DataFrame)->Generator[Tuple[str,str],None,None]:
    for (index,row) in df.iterrows():
        yield (row["FILE_ID"],row["PATH"])
if __name__ =="__main__":
    # Trace 
    trace_path = os.environ.get("TRACE_PATH","/test/out/trace.csv")
    # Space-separated string that contains basic info of the peers.
    peers_str = os.environ.get("MICTLANX_PEERS","mictlanx-peer-0:localhost:7000")
    # Parse the peers_str to a Peer object
    peers = list(Utils.peers_from_str(peers_str))
    # Create an instance of MictlanX - Client 
    L.debug("TRACE_PATH={}".format(trace_path))
    c = Client(
        # Unique identifier of the client
        client_id   = os.environ.get("MICTLANX_CLIENT_ID","client-0"),
        # Storage peers
        peers       = peers,
        # Number of threads to perform I/O operations
        max_workers = int(os.environ.get("MICTLANX_MAX_WORKERS","2")),
        # This parameters are optionals only set to True if you want to see some basic metrics ( this options increase little bit the overhead please take into account).
        debug       = True,
        daemon      = True, 
        # ____________
    )
    ## <default> 2 uploads per unit of time (seconds)
    arrival_rate = int(os.environ.get("ARRIVAL_RATE","2"))
    ## Average interarrival time
    y_lambda     =  1/arrival_rate 
    # Generated exponential distributed interarrival times using <y_lambda>
    interarrival_times_dist = S.expon(loc = y_lambda, scale=1)
    # Get the source folder from the envs
    source_folder = os.environ.get("SOURCE_FOLDER","/test/out")
    # Create a empty list of Futures
    futures:List[Awaitable[Result[PutResponse,Exception]]] =[]
    # Start time 
    start_time = T.time()
    # Select the row generator from using os.walk or a trace file. 
    if len(trace_path) == 0:
        row_source_generator = generate_filename_path_from_os_walk(source_folder=source_folder)
    else:
        df            = pd.read_csv(trace_path)
        row_source_generator = generate_rows_from_df(df = df)

    try:
        total_operations = 0
        success_counter =0
        # Walk of the <row_source>
        
        xs = list(row_source_generator)
        print(len(xs))
        # for (filename,file_path) in row_source_generator:
        for (filename,file_path) in xs:
            total_operations+=1
            # Read the file in binary mode 
            with open(file_path,"rb") as f:
                # Read the whole file in to RAM (consider using chunks for larger files)
                data = f.read()
                # Perform a put operation in MictlanX
                future                          = c.put(
                    # bytes of the data
                    value=data,
                    # Used-defined metadata of the data
                    tags={
                        "example_name":"01_put_bulk",
                        "some_str_json":J.dumps({"field1":"1","field2":"FIELD"}),
                        "exmaple_tag_key":"EXAMPLE_TAG_VALUE",
                        "FILE_PATH":file_path,
                        "FILE_SIZE":str(len(data))
                    },
                    bucket_id=os.environ.get("MICTLANX_BUCKET_ID","MICTLANX_GLOBAL_BUCKET")
                )
                # Insert the current future
                futures.append(future)
                T.sleep(interarrival_times_dist.rvs())
            
            # Drain all futures (timeout=None means wait until all completed or fail)
        for future in as_completed(futures,timeout=None):
            result = future.result()
            if result.is_ok:
                print(success_counter,result.unwrap())
                success_counter+=1
        
        end_time = T.time()
        print("TOTAL_OEPRATIONS {}".format(total_operations))
        print("SUCCES_OEPRATIONS {}".format(success_counter))
        failure_rate = ((total_operations - success_counter) / total_operations)*100
        success_rate = 100 - failure_rate
        total_time = end_time -start_time
        print("TOTAL_TIME: {}s SUCCESS_RATE={}% FAILURE_RATE={}%".format(total_time,success_rate,failure_rate))
    except Exception as e:
        L.error(str(e))
    finally:
        c.shutdown()