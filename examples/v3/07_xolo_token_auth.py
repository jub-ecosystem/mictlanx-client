import sys
import os
from mictlanx.v3.interfaces.payloads import SignUpPayload,AuthTokenPayload,VerifyTokenPayload,RefreshTokenPayload,LogoutPayload
from mictlanx.v3.services.xolo import Xolo
from dotenv import load_dotenv
from option import Some,NONE
import requests as R

load_dotenv()
if __name__ =="__main__":
    args      = sys.argv[1:]
    if(len(args) >= 4  or len(args)==0):
        raise Exception("Please try to pass a valid file path: python examples/v3/04_xolo.py <APP_ID> <CLIEND_ID> <SECRET>")
    app_id    = args[0]
    client_id = args[1]
    secret    = args[2]
    # _________________________
    # 1. Create an instance of Xolo.
    xolo           = Xolo(ip_addr =os.environ.get("MICTLANX_XOLO_IP_ADDR"), port = int(os.environ.get("MICTLANX_XOLO_PORT")), api_version = Some(int(os.environ.get("MICTLANX_API_VERSION"))))
    # 2. Perform a Sign up operation that create a new user linked to an application.
    signup_payload = SignUpPayload(app_id=app_id, client_id=client_id, secret=secret, metadata={"first_name":"Ignacio", "last_name": "Castillo","age":"21"})
    signup_result  = xolo.signup(signup_payload)
    # 3. Check if the signup operation was ok. 
    # 3.1 If the signup is ook then extract the response safely :) using an Result(for more info check https://doc.rust-lang.org/std/result/ )
    if(signup_result.is_ok):
        signup_response = signup_result.unwrap()
        print("SIGNUP_RESULT",signup_response)
        # 4. Perform a Auth operation that authenticate a user linked to an application.
        auth_payload = AuthTokenPayload(app_id=app_id,client_id=client_id,secret=secret,expires_in=NONE) # if expires_in is NONE then the default value will be 1 day in seconds 
        auth_result   = xolo.auth(auth_payload)
        # 4.1 If the auth is ok then extract the response safely :)  and print it out
        print("AUTH_RESULT",auth_result)
        if(auth_result.is_ok):
            auth_response = auth_result.unwrap()
            # 5. Perform token verification and get the result. (this operation never return an error at least the Xolo server is crashed)
            verify_token_payload = VerifyTokenPayload(app_id=app_id,client_id=client_id,token=auth_response.token,secret=secret)
            verify_result = xolo.verify_token(verify_token_payload)
            print("VERIFY_RESULT",verify_result)
            # 6. Refresh token 
            refresh_token_payload = RefreshTokenPayload(app_id=app_id,client_id=client_id,token=auth_response.token,secret=secret)
            refresh_token_result  = xolo.refresh_token(refresh_token_payload) 
            print("REFRESH_TOKEN_RESULT",refresh_token_result)
            # Here we check that the refresh token operation was successfully then logout using the new token.
            if(refresh_token_result.is_ok):
                refresh_token_response = refresh_token_result.unwrap()
                # 7. Logout
                logout_payload = LogoutPayload(app_id=app_id,client_id=client_id,token=refresh_token_response.token,secret=secret)
                logout_result  = xolo.logout(logout_payload)
                print("LOGOUT_RESULT",logout_result)
            else:
                error = refresh_token_result.unwrap_err()
                error_response:R.Response = error.response
                print(error,error_response.headers)

        # 4.2 If the result was not ok :( then extract the error safely and get the headers to show the error message
        else:
            error = auth_result.unwrap_err()
            error_response:R.Response = error.response
            print(error,error_response.headers)

    # 3.2 If the result was not ok :( then extract the error safely and get the headers to show the error message
    else:
        error = signup_result.unwrap_err()
        error_response:R.Response = error.response
        print(error,error_response.headers)

    # print(app_id,client_id,secret)
    