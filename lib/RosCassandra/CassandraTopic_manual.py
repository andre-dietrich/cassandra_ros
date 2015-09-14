from CassandraTopic_ import *

class CassandraTopic_manual(CassandraTopic_ros):
    def __init__(self, MsgClass):        
        CassandraTopic_ros.__init__(MsgClass)
    
    def getColumnValidationClasses(self):
        
        classes = {'.transforms.header' : 'ros',
                   '.transforms.child_frame_id' : 'ros',
                   '.transforms.transform' : 'string' }
        
        spec = super(CassandraTopic_manual, self).getColumnValidationClasses()
        
        # delete the plain ros stuff, because it is already inserted
        for cls in classes.keys():
            if classes[cls] == 'ros':
                classes.pop(cls)
        
        # remove the non ros stuff from the validation classes ...
        for key in spec.keys():
            for cls in classes.keys():
                if key.find(cls) == 0:
                    spec.pop(cls)
        
        # insert the missing validation classes
        for cls in classes.keys():
            spec[cls] = classes[cls]
            
        self.column_validation_classes = spec
                    
        return spec
         
    def encode(self, msg):
        return self._ros_to_cassandra(msg)
    
    def _value_to_cassandra(self ,data, parent):
        
        if self.column_validation_classes.has_key(parent):
            if self.column_validation_classes[parent] == 'string':
                return { parent : rosjson.ros_message_to_json(data) }
        
        return super(CassandraTopic_manual, self)._value_to_cassandra(self ,data, parent)

    
#   def decode(self, data):
#        data = json.loads(data['manual'])
