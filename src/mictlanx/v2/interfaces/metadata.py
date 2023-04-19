from typing import Dict
class Metadata(object):
    def __init__(self,**kwargs):
        self.id:str             = kwargs.get("id","BALL_ID")
        self.size:int           = kwargs.get("size",0)
        self.checksum:str       = kwargs.get("checksum","CHECKSUM")
        self.tags:Dict[str,str] = kwargs.get("tags",{})
        # self.reads:int          = kwargs.get("reads",0)
        self.created_at:int     = kwargs.get("created_at",0)
        # self.last_read:int      = kwargs.get("last_read",0)
    
    def empty(**kwargs):
        return Metadata(**kwargs)
    def to_dict(self):
        return self.__dict__

    def __str__(self)->str:
        return "Metadata(id={}, size={}, checksum={}, created_at={})".format(self.id,self.size,self.checksum,self.created_at)