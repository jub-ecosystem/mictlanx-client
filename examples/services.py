# import socket as S
# from mictlanx.v2.interfaces import requests
# from mictlanx.v2.interfaces import responses
# from mictlanx.v2.codec.codec import ClientCodec
from mictlanx.logger.log import Log
from option import Result,Err,Ok
from mictlanx.v2.services.auth import AuthService
from mictlanx.v2.services.replica_manager import ReplicaManagerService
from mictlanx.v2.services.storage_node import StorageNodeService
from uuid import uuid4 
# from typing import Optional
import time as T
import numpy as np
# from abc import ABC 
value = np.ones((10,10)).tobytes()

def test1():
    MAX_CLIENTS = 1
    auth_service = AuthService(ip_addr = "localhost",port=10000)
    rm_service   = ReplicaManagerService(ip_addr = "localhost", port=20000)

    for j in range(MAX_CLIENTS):
        client_id = "client-{}".format(j)
        # 1. GENERATE TOKEN
        generate_token_result = auth_service.generate_token(client_id = client_id, password = "topSecret", headers={})
        if(generate_token_result.is_ok):
            # 2. GENERATE TOKEN RESPONSE
            generate_token_response = generate_token_result.unwrap()
            print("GENERATE_RESPONSE[{}]".format(j),generate_token_response)
            # 3. Balance
            fx_headers = lambda operation,key:{
                "token":generate_token_response.token, 
                "jti":generate_token_response.json_web_token_id,
                "sub":client_id,
                "operation":operation,
                "key":key, 
                "size":"1000"
            }
            MAX_REQUESTS = 10
            for j in range(MAX_REQUESTS):
                key ="ball-"+str(uuid4())
                balance_result = rm_service.balance(headers=fx_headers("PUT",key))
                if(balance_result.is_ok):
                    balance_response = balance_result.unwrap()
                    storage_node     = StorageNodeService(ip_addr = balance_response.ip_addr, port = balance_response.port)
                    sn_result = storage_node.put(key=key, value = value, headers={"client_id": client_id, "token": generate_token_response.token,"to_disk":"1","tags":{"tag_key_1":"tag_value1","test":"test"}})
                    print("BALANCE_RESPONSE[{}]".format(j),balance_response)
                    if(sn_result.is_ok):
                        sn_response = sn_result.unwrap()
                        print("SN_RESPONSE[{}]".format(j),sn_response)
                        # _______________________________________________________
                    else:
                        print("SN_ERROR[{}]".format(j),sn_result)
                    storage_node.exit()
                else:
                    print("BALANCE_ERROR[{}]".format(j),balance_result.unwrap_err())
                T.sleep(0.5)
                print("_"*20)
                print("INIT_GET")
                for k in range(10):
                    balance_result = rm_service.balance(headers=fx_headers("GET",key))
                    print("GET_BALANCE_RESULT[{},{}]".format(j,k),balance_result)
                    if(balance_result.is_ok):
                        balance_response = balance_result.unwrap()
                        storage_node     = StorageNodeService(ip_addr = balance_response.ip_addr, port = balance_response.port)
                        get_result = storage_node.get(key =key, headers={"client_id": client_id, "token": generate_token_response.token})
                        print("GET_STORAGE_NODE_RESULT[{},{}]".format(j,k),get_result)
                        if(get_result.is_ok):
                            get_response = get_result.unwrap()
                            print("GET_RESPONSE[{},{}]".format(j,k),get_response)
                        else:
                            print("SN_GET_ERROR[{},{}]".format(j,k), get_result.unwrap_err())
                        T.sleep(1)
                        storage_node.exit()
                    else:
                        print("BALANCE_ERROR[{},{}]".format(j,k),balance_result.unwrap_err())
                    print("_"*20)
            # print(balance_result)
        else:
            print("AUTH_ERROR",generate_token_result)
        auth_service.exit()
        rm_service.exit()

test1()
        # if(responses.MaxClientReached.check(generate_token_response)):
            # print("MAX_CLIENTS_REACHED...")
            # continue
        # else:
            # print("_"*20)
            # 3. BALANCE
            # balance_req = requests.Balance(
            # )
            # rm_socket   = S.socket(S.AF_INET, S.SOCK_STREAM)
            # rm_socket.connect(("localhost",20000))
            # total_balance_reqs = np.random.poisson(lam=2, size=10)
            # for i,iat in enumerate(total_balance_reqs):
                # rm_socket.sendall(balance_req.encode())
                # balance_response = ClientCodec.decode(socket=rm_socket)

                # print("BALANCE_RESPONSE[{}]".format(i),balance_response)
                # print("_"*20)
                # T.sleep(iat)
            # exit_req = requests.Exit()
            # rm_socket.sendall(exit_req.encode())
            # auth_socket.sendall(exit_req.encode())
            # exit_res = ClientCodec.decode(socket = rm_socket)
            # print("EXIT_RESPONSE",exit_res)
        # 3.1 VERIFY TOKEN 
        # verify_token_req = requests.VerifyToken(
        #     client_id =client_id,
        #     token=generate_token_response.token,
        #     json_web_token_id=generate_token_response.json_web_token_id
        # )
        # socket.sendall(verify_token_req.encode())
        # # 3.2 VERIFY TOKEN RESPONSE
        # response = ClientCodec.decode(socket = socket)
        # print("VERIFY_TOKENRESPONSE",response)
        # print("_"*20)
        # rm_socket.close()
        # auth_socket.close()
        # T.sleep(1)

# rm_socket   = S.socket(S.AF_INET, S.SOCK_STREAM)
# rm_socket.connect(("localhost",20000))
# T.sleep(5)
        



# auth = AuthService(ip_addr = "localhost",port = 10000)
# response = auth.generate_token(client_id="client-0",password="topSecret")
# if(response.is_ok):
#     print("RESPONSE",response)
#     T.sleep(5)
#     auth.exit()
# rm   = ReplicaManagerService(ip_addr = "localhost", port = 20000)

# T.sleep(5)
# response = rm.exit()
# auth
            



# auth_socket = S.socket(S.AF_INET,S.SOCK_STREAM)
# auth_socket.connect(("localhost",10000))
# exit_req = requests.Exit()
# rm_socket.sendall(exit_req.encode())
# exit_res = ClientCodec.decode(socket = rm_socket)
# print("EXIT_RESPONSE",exit_res)
# print("CLOSE_RM")
# rm_socket.shutdown(q)
# rm_socket.close()