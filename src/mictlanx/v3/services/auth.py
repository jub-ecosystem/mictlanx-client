import requests as R
from mictlanx.v3.interfaces.service import Service
from mictlanx.v3.interfaces.payloads import GenerateTokenPayload 
from mictlanx.v3.interfaces.responses import GenerateTokenResponse
class Auth(Service):
    def __init__(self,*args,**kwargs):
        super(Auth,self).__init__(*args,**kwargs)
        self.generate_token_url = '{}/auth'.format(self.base_url)
    def generate_token(self,payload:GenerateTokenPayload):
        try:
            response_data = payload.to_dict()
            response = R.post(self.generate_token_url,json=response_data)
            response_data = GenerateTokenResponse(**response.json())
            return response_data
        except Exception as e:
            print("ERROR {}",e)
            raise e