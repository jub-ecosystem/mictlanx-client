from typing import List,Dict,Set
from option import Option,NONE,Some
from threading import Thread
from mictlanx.v3.services.xolo import Xolo
import json as J
import time as T

class Acl(object):
    def __init__(self,roles:Set[str] = set(), resources:Set[str] = set(), permissions:Set[str] = set(), grants:Dict[str, Dict[str, Set[str]]] = {}):
        self.__roles                                 = roles
        self.__resources                             = resources
        self.__permissions                           = permissions
        self.__grants:Dict[str, Dict[str, Set[str]]] = grants
        self.__daemon                                = Thread(target=self.__run,name="XoloDaemon", daemon=True)
        self.__daemon.start()

    def __run(self):
        while True:
            print("Sync with Xolo server...")
            T.sleep(5)
    # Create
    def add_role(self,role:str):
        if not role in self.__roles:
            self.__roles.add(role)
            self.__grants[role] = {}
    
    def add_resource(self,resource:str):
        if not resource in self.__resources:
            self.__resources.add(resource)
    
    def add_permission(self,permissions:str):
        if not permissions in self.__permissions:
            self.__permissions.add(permissions)
    
    def add(self, grants:Dict[str,Dict[str,Set[str]]]):
        for key,val in grants.items():
            self.__roles.add(key)
            for resource, permissions in val.items():
                self.__resources.add(resource)
                self.__permissions.union(set(permissions))
        # self.grants= {**self.grants, **grants}
    # Remove
    def remove_role(self,role:str):
        if role in self.__grants:
            self.__grants.pop(role)
        self.__roles.discard(role)
    
    def remove_resource(self,resource:str):
        for key,value in self.__grants.items():
            if resource in value:
                self.__grants[key].pop(resource)
        self.__resources.discard(resource)

    def remove_permission(self,permission:str):
        for key,value in self.__grants.items():
            for resource,permissions in value.items():
                ps = set(permissions)
                ps.discard(permission)
                self.__grants[key][resource] = ps
        self.__permissions.discard(permission)
    # Get
    def get_roles(self)->Set[str]:
        return self.__roles
    
    def get_resources(self)->Set[str]:
        return self.__resources
    
    def get_permissions(self)->Set[str]:
        return self.__permissions
    # Authorized
    def grant(self,role:str, resource:str, permission:str):
        self.__roles.add(role)
        self.__resources.add(resource)
        self.__permissions.add(permission)
        defaultv =  {resource: set([permission])}
        self.__grants.setdefault(role,defaultv)
        self.__grants[role][resource].add(permission)
    
    def grants(self, grants:Dict[str, Dict[str, Set[str]]]):
        self.add(grants=grants)
        self.__grants= {**self.__grants, **grants}
    
    def revoke(self, role:str, resource:str, permission:str):
        if role in self.__grants:
            xs = self.__grants[role]
            if resource in xs:
                perms = set(self.__grants[role][resource])
                perms.discard(permission)
                self.__grants[role][resource] = perms
    
    def revoke_all(self, role:str, resource:Option[str]= NONE):
        if role in self.__grants:
            if resource.is_none:
                self.__grants[role] = {}
            else:
                resource = resource.unwrap()
                if resource in self.__resources:
                    self.__grants[role][resource.unwrap()] = set()
    #  Check
    def check(self,role:str, resource:str, permission:str)->bool:
        _role_perms = self.__grants.get(role,{})
        _resource_perms = _role_perms.get(resource,set())
        return permission in _resource_perms
    
    def check_any(self,roles:List[str], resource:str, permission:str)->bool:
        for role in roles:
            x = self.__grants.get(role, {})
            y = x.get(resource,set())
            if permission in y:
               return True 
        return False
    
    def check_all(self,roles:List[str], resource:str, permission:str)->bool:
        res = []
        for role in roles:
            x = self.__grants.get(role, {})
            y = x.get(resource,set())
            if permission in y:
                res.append(True)
        return len(res) == len(roles)
    
    def which_permissions(self, role,resource)->Set[str]:
        return self.__grants.get(role,{}).get(resource,set())

    def show(self)->Dict[str, Dict[str, Set[str]]]:
        return self.__grants

    def save(self,key:str,path:str):
        try:
            xolo = Xolo()
            secret_key            = bytes.fromhex(key)
            grants = {}
            for role,resources_perms_map in self.__grants.items():
                for resource,permissions in resources_perms_map.items():
                    if not role in grants:
                        grants[role] ={}
                    if not resource in grants[role]:
                        grants[role][resource] = list(permissions)
            obj = {
                "roles":list(self.__roles),
                "resources": list(self.__resources),
                "permissions": list(self.__permissions),
                "grants": grants
            }
            json_str = J.dumps(obj)
            res = xolo.encrypt_aes(key=secret_key, data= json_str.encode("utf-8"))
            if res.is_ok:
                with open(path,"wb") as f:
                    f.write(res.unwrap())
            else:
                print("Something went wrong {}".format(res.unwrap_err()))

        except Exception as e:
            print(e)
        # with open(path, "w") as f:
            # J.dump(obj, f)

    def load(key:str, path:str) -> Option["Acl"]:
        try:
            with open(path, "rb") as f:
                xolo = Xolo()
                secret_key            = bytes.fromhex(key)
                data = f.read()
                res = xolo.decrypt_aes(key=secret_key, data=data)
                if res.is_ok:
                    obj = J.loads(res.unwrap().decode("utf-8"))
                    grants = obj.get("grants",{})
                    for k1,v1 in grants.items():
                        for k2,v2 in v1.items():
                            grants[k1][k2] = set(v2)

                    acl = Acl(
                        roles       = set(obj.get("roles",set())),
                        resources   = set(obj.get("resources",set())),
                        permissions = set(obj.get("permissions",set())),
                        grants      = grants
                    )
                    return Some(acl)
                raise res.unwrap_err()
        except Exception as e:
            print(e)
            return NONE
                
            # json_str = res.
            # obj = J.load(f)
            # return acl
            

if __name__ =="__main__":
    acl = Acl()
    acl.add(grants= {
        "admin":{
            "bucket-0":["write","read"]
        },
        "user":{
            "bucket-1":["read","delete"]
        }
    })
    acl.remove_permission("read")

    acl.grant("guest","bucket-2","read")
    # acl.remove_role("admin")
    print("Roles",acl.get_roles())
    print("Resources",acl.get_resources())
    print("Permissions",acl.get_permissions())
    print("Grants",acl.show())
    acl.grants(grants= {
        "admin":{
            "bucket-0":["write","read"]
        },
        "user":{
            "bucket-1":["read","delete"]
        }
    })
    acl.revoke("user","bucket-1","read")
    acl.revoke_all("user")
    print("Grants",acl.show())
    print(acl.check("admin","bucket-0","read"))
    print(acl.check("admin","bucket-0","update"))
    acl.save(key= "913c839ae0d9d6a72ec96b8b383c18c57f4aeac98f9730511d3b48f9e2680b01",path="/sink/mictlanx-acl2")
    acl2 = Acl.load(key="913c839ae0d9d6a72ec96b8b383c18c57f4aeac98f9730511d3b48f9e2680b01", path="/sink/mictlanx-acl2")
    if acl2.is_some:
        print(acl2.unwrap().show())
    else:
        print("Error loading acl")

    # print("ACL2",acl2.show())
    T.sleep(20)