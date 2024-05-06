import os
import unittest as UT
import secrets
import time as T
import numpy as np
from typing import Generator
from option import Some
from mictlanx.utils.index import Utils
from mictlanx.v4.client import Client
import pandas as pd
from dotenv import load_dotenv
from mictlanx.logger.tezcanalyticx.tezcanalyticx import TezcanalyticXParams
from concurrent.futures import ThreadPoolExecutor,as_completed
from mictlanx.utils.segmentation import Chunks
import logging
from scipy import stats as S
import random

load_dotenv()


class MictlanXTest(UT.TestCase):
    BUCKET_ID = os.environ.get("BUCKET_ID","mictlanx")
    peers =  Utils.routers_from_str(
        routers_str=os.environ.get("MICTLANX_ROUTERS","mictlanx-router-0:localhost:60666"),
        protocol=os.environ.get("MICTLANX_PROTOCOL","http")
    ) 
    ylambda = 1
    dist = S.expon(ylambda)
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
        log_output_path= os.environ.get("MICTLANX_CLIENT_LOG_PATH","/mictlanx/client"),
        tezcanalyticx_params=Some(
            TezcanalyticXParams(
                flush_timeout="30s",
                buffer_size=10,
                hostname="localhost",
                protocol="http",
                level=logging.INFO
            )
        )
    )
    @staticmethod
    def data_generator(num_chunks:int,n:int)->Generator[bytes,None,None]:
        for i in range(num_chunks):
            yield secrets.token_bytes(n)
    
    

        
    @UT.skip("")
    def test_chunks_from_ndarray(self):
        ndarray = np.random.rand(2,5,3)*1000000
        print(ndarray.shape)
        maybe_chunks = Chunks.from_ndarray(
            ndarray= ndarray,
            group_id = "x",
            chunk_prefix = Some("x"),
            num_chunks = 10

        )
        print(maybe_chunks)
        if maybe_chunks.is_some:
            chunks = maybe_chunks.unwrap()
            for chunk in chunks.iter():
                print(chunk.to_ndarray().unwrap().shape)

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
            bucket_id="mictlanx",
            key="5c0ea09626a3796b99a8e030e6e302f8106ac2443add951c87550034e966408f"
        )
        print("DEL RESPONSE", res)
        return self.assertTrue(res.is_ok)
    
    @UT.skip("")
    def test_delete_by_bid(self):
        res = MictlanXTest.client.delete_by_ball_id(
            bucket_id="mictlanx",
            ball_id="5c0ea09626a3796b99a8e030e6e302f8106ac2443add951c87550034e966408f"
        )
        print("RES",res)
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
        test_str = "mY_DATASUP0124"
        res = Utils.sanitize_str(x=test_str)
        print(res)
        return self.assertTrue(res == "mydatasup0124")
    
    @UT.skip("")
    def test_delete_bucket(self):
        bucket_id = "test"
        res = MictlanXTest.client.delete_bucket(bucket_id=bucket_id)
        return self.assertTrue(res.is_ok)
    
    @UT.skip("")
    def test_put_chunked(self):
        MAX_PUTS = 1000
        for i in range(MAX_PUTS):
            chunks = MictlanXTest.data_generator(num_chunks=10, n = 1000)
            res = MictlanXTest.client.put_chunked(chunks= chunks)
            print(res)
            T.sleep(2)
        return self.assertTrue(res.is_ok)
    
    def generate_gets(self, bucket_id:str, key:str, get_counter:int= 1):
        x = self.dist.rvs()
        # access_counts = np.random.zipf(a = 1.5, size=1)
        # access_counts = np.clip(access_counts, a_min=None, a_max=100)
        # get_counter = access_counts[0]
        print("{}@{} {} gets".format(bucket_id,key,get_counter))
        for i in range(get_counter):
            res = MictlanXTest.client.get(key=key,bucket_id=bucket_id).result()
            T.sleep(x)
            if res.is_ok:
                print("GET[{}] {}".format(i,key))

    @UT.skip("")
    def test_experiment_basic(self):
        MAX_ITERATIONS = 1000
        max_workers = 4
        num_chunks = 10
        n_file_size = 10000
        bucket_id = "test"
        objects = [
            "mictlanxobjecttest1",
            "mictlanxobjecttest2",
            "mictlanxobjecttest3",
            "mictlanxobjecttest4",
            "mictlanxobjecttest5",
        ]
        objects_probabilities = [
            0.1,0.1,0.1,0.15,.65
        ]


        with ThreadPoolExecutor(max_workers=max_workers) as tp :
            n_puts = 0
            n_objects = len(objects)
            i = 0 
            np.random.seed(123)
            access_counter = np.sort(np.clip(np.random.zipf(1.5,size=n_objects), a_min=None, a_max=100))
            while n_puts < n_objects or  i < MAX_ITERATIONS:
                if i % n_objects ==0:
                    access_counter = np.sort(np.clip(np.random.zipf(1.5,size=n_objects), a_min=None, a_max=100))
                if n_puts < n_objects:
                    chunks = MictlanXTest.data_generator(num_chunks=num_chunks, n = n_file_size)
                    res = MictlanXTest.client.put_chunked(
                        bucket_id=  bucket_id,
                        key=objects[i % n_objects],
                        chunks= chunks
                    )
                    if res.is_ok:
                        key =  res.unwrap().key
                        tp.submit(self.generate_gets, bucket_id = bucket_id, key = key, get_counter = access_counter[i % n_objects])
                    n_puts+=1
                else:
                    key = random.choices(objects, objects_probabilities)[0]
                    tp.submit(self.generate_gets, bucket_id = bucket_id, key = key , get_counter = access_counter[i % n_objects])
                i+=1
                iat = self.dist.rvs()
                T.sleep(iat)
        return self.assertTrue(res.is_ok)
    

if __name__ == "__main__":
    UT.main()