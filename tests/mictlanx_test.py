import os
import unittest as UT
import secrets
import time as T
import numpy as np
from typing import Generator
from mictlanx.utils.index import Utils
from mictlanx.v4.client import Client
import pandas as pd
from dotenv import load_dotenv

load_dotenv()


class MictlanXTest(UT.TestCase):
    BUCKET_ID = os.environ.get("BUCKET_ID","mictlanx")
    peers =  Utils.routers_from_str(
        routers_str=os.environ.get("MICTLANX_ROUTERS","mictlanx-router-0:localhost:60666"),
        protocol=os.environ.get("MICTLANX_PROTOCOL","http")
    ) 
    # bucket_id = "public-bucket-0"

    
    client = Client(
        client_id    = os.environ.get("CLIENT_ID","client-0"),
        # 
        routers        = list(peers),
        # 
        debug        = True,
        # 
        max_workers  = 2,
        # 
        lb_algorithm ="2CHOICES_UF",
        bucket_id= BUCKET_ID,
        log_output_path= os.environ.get("MICTLANX_CLIENT_LOG_PATH","/mictlanx/client")
    )
    @staticmethod
    def data_generator(num_chunks:int,n:int)->Generator[bytes,None,None]:
        for i in range(num_chunks):
            yield secrets.token_bytes(n)
    
    @UT.skip("")
    def test_bulk_put_from_csv(self):
        df = pd.read_csv("/source/contaminantes.csv")
        for index,row in df.iterrows():
            try:
                result = MictlanXTest.client.put_file_chunked(
                    path=row["PATH"],
                    tags={}
                )
                if result.is_ok:
                    response = result.unwrap()
                    print(row["LEVEL1"],".",row["LEVEL2"], "put successfully {}".format(response.key))
            except Exception as e:
                print(e)

    @UT.skip("")
    def test_put_ndarray(self):
        nd_array =np.random.randint(10,20,(100,100)) 
        result = MictlanXTest.client.put_ndarray(
            ndarray=nd_array,
            tags={},
            key="matrixxxxxxxxxxxxxxxxx",
            bucket_id="mictlanx"
            )
        print(result)
    @UT.skip("")
    def test_get_ndarray(self):
        nd_array =np.random.randint(10,20,(100,100)) 
        result = MictlanXTest.client.get_ndarray(key="matrixxxxxxxxxxxxxxx")
        print(result)


    @UT.skip("")
    def test_put_file_chunked(self):
        res = MictlanXTest.client.put_file_chunked(
            path="/source/hugodata.csv",
            key="my_hugo_DATA-123"
        )
        T.sleep(1)
        return self.assertTrue(res.is_ok)

    @UT.skip("")
    def test_delete(self):
        res = MictlanXTest.client.delete(
            bucket_id="activex",
            key="2a45605714f82cd082c0b607cca6b0aff36ba5383a8498021be7fa8328e8e3ac"
        )
        return self.assertTrue(res.is_ok)
    
    @UT.skip("")
    def test_get_to_file(self):
        res = MictlanXTest.client.get_to_file(
            key="2a45605714f82cd082c0b607cca6b0aff36ba5383a8498021be7fa8328e8e3ac",
            bucket_id="activex",
            output_path="/activex/data"
        )
        return self.assertTrue(res.is_ok)

    @UT.skip("")
    def test_extract_file_info(self):
        fullname,filename,ext = Utils.extract_path_info(path="/source/hugodata.csv")
        print("FULLNAME",fullname)
        print("FILENAME",filename)
        print("EXT",ext)
        return self.assertTrue(fullname == "hugodata.csv" and filename =="hugodata" and ext =="csv")
    
    @UT.skip("")
    def test_str_satinize(self):
        test_str = "my_DAtasup0124"
        res = Utils.sanitize_str(x=test_str)
        print(res)
        return self.assertTrue(res == "mydatasup0124")
    
    @UT.skip("")
    def test_put_chunked(self):
        MAX_PUTS = 100
        for i in range(MAX_PUTS):
            chunks = MictlanXTest.data_generator(num_chunks=100000, n = 10)
            res = MictlanXTest.client.put_chunked(chunks= chunks)
            T.sleep(1)
        return self.assertTrue(res.is_ok)
    

if __name__ == "__main__":
    UT.main()