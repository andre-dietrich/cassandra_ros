import roslib; roslib.load_manifest("cassandra_ros")
import rospy
#import rosjson
from rospy_message_converter import json_message_converter
import genpy

import pycassa
import simplejson as json

from CassandraTopic_ import *

class CassandraTopic_string(CassandraTopic_):
    def __init__(self, MsgClass):        
        CassandraTopic_.__init__(MsgClass)
        
    def getColumnValidationClasses(self):
        return {'string' : pycassa.UTF8_TYPE}
         
    def encode(self, msg):
        #return {'string' : rosjson.ros_message_to_json(msg)}
        return {'string' : json_message_converter.convert_json_to_ros_message(self.MsgClass()._type, msg)}

    def decode(self, data):
        _dict = json.loads(data['string'])
        msg = self.MsgClass()
        msg = self._dict_to_ros(_dict, msg)
        return msg
    
    def _dict_to_value(self, _dict, _slot, _type):
        
        if isinstance(_slot, (str, int, float, bool)):
            return _dict
        
        elif isinstance(_slot, (list, dict, tuple)):
            msg = []
            if _type in ('bool[]', 'int8[]', 'uint8[]', 'int16[]', 'uint16[]', 'int32[]', 'uint32[]',
                         'int64[]', 'uint64[]', 'float32[]', 'float64[]', 'string[]'):
                msg = _dict
            else:
                _type = _type.replace("[]","")
                _slot = genpy.message.get_message_class(_type)()
                for elem in _dict:
                    msg.append( self._dict_to_ros( elem, _slot) )
                    
            return msg
        elif isinstance(_slot, (genpy.rostime.Time, genpy.rostime.Duration)):
            t = rospy.Time()
            secs = int(_dict)
            nsecs= (_dict - float(secs))*10**9
            t.set(secs, nsecs)
            return t
        elif isinstance(_slot, rospy.Message):
            return self._dict_to_ros(_dict, _slot)
        else:
            raise RosCassandraTranspileException("unknown type: %s"%type(_slot)) 
        
    def _dict_to_ros(self, _dict, msg):
        
        for i in range(len(msg.__slots__)):
            _slot = msg.__slots__[i]
            _type = msg._slot_types[i]
            
            setattr(msg, 
                    _slot,
                    self._dict_to_value( _dict[_slot],
                                        getattr(msg, _slot),
                                        _type))
    
        return msg