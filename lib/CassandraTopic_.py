class CassandraTopic_(object):
    def __init__(self, MsgClass):
        self.MsgClass = MsgClass
        
    def getColumnValidationClasses(self):
        return {}
        
    def encode(self, msg):
        pass
    
    def decode(self, data):
        pass
