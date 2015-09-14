import pycassa
import yaml

from CassandraTopic_ import *

class CassandraTopic_yaml(CassandraTopic_):
    def __init__(self, MsgClass):        
        CassandraTopic_.__init__(MsgClass)
        
    def getColumnValidationClasses(self):
        return {'yaml' : pycassa.BYTES_TYPE}
    
    def encode(self, msg):
        return {"yaml" : str(yaml.dump(msg))}
    
    def decode(self, data):
        return yaml.load(data["yaml"])
