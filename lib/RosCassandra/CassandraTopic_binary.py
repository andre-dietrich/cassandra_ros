import pycassa
import StringIO

from CassandraTopic_ import *

class CassandraTopic_binary(CassandraTopic_):
    def __init__(self, MsgClass):        
        CassandraTopic_.__init__(MsgClass)
    
    def getColumnValidationClasses(self):
        return {'binary' : pycassa.BYTES_TYPE}
    
    def encode(self, msg):
        buffer = StringIO.StringIO()
        msg.serialize(buffer)
        return {"binary" : buffer.getvalue()}
    
    def decode(self, data):
        msg = self.MsgClass()
        msg.deserialize(data["binary"])
        return msg
