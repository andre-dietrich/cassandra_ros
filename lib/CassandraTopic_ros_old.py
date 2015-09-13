from CassandraTopic_ import *

import std_msgs.msg
import rosmsg

class CassandraTopic_ros_old(CassandraTopic_):
    def __init__(self):        
        CassandraTopic_.__init__(MsgClass)
        
    def encode(self, msg):
        return self.ros_to_cassandra(msg)
    
    def decode(self, data):
        msg = self.MsgClass()
        _dict = self.cassandra_to_dict(data)
        msg = self.dict_to_ros(_dict, msg)
        return msg
    
    def value_to_cassandra(self ,data, parent):

        if type(data) == str:
            return { parent : data }
        elif type(data) in (int, float, long):
            return { parent : str(data) }
        elif type(data) == bool:
            return self.value_to_cassandra(int(data))
        elif isinstance(data, (genpy.rostime.Time, genpy.rostime.Duration)):
            return { parent+'.secs':str(data.secs), parent+'.nsecs':str(data.nsecs)}
        elif type(data) in (list, tuple):
            _dict = {}
            for i in range(len(data)):
                res = self.value_to_cassandra(data[i], parent+"["+str(i)+"]")
                for key in res.keys():
                    _dict[key] = res[key]
            return _dict
        elif isinstance(data, rospy.Message):
            return self.ros_to_cassandra(data, parent)
        else:
            raise RosCassandraTranspileException("unknown type: %s"%type(data)) 
    
    ################################################################################################################    
    def ros_to_cassandra(self, data, parent=""):
        if not isinstance(data, rospy.Message):
            #raise RosCassandraTranspileException("not a valid rospy Message instance: %s"%data.__class__.__name__)
            return self.value_to_cassandra(data, parent)
    
        _dict = {}
    
        if len(parent) > 0:
            parent += "."        
        
        for elem in data.__slots__:
            sub_elements = self.value_to_cassandra( getattr(data, elem) , parent+elem)
            
            if type(sub_elements) == dict:
                for key in sub_elements.keys():
                    _dict[key] = sub_elements[key]
                
        return _dict
    
    ################################################################################################################
    
    def value_to_dict(self, name, value, _dict):
        
        # treat as array
        if name[0][-1] == ']':
            ar = name[0].find('[')
            element = int(name[0][ar+1:-1])
            name[0] = name[0][:ar]
            
            # it is already an array
            if _dict.has_key(name[0]):
                if len(_dict[name[0]]) == element:
                    _dict[name[0]].append(self.value_to_dict(name[1:], value, {}))
                else:
                    _dict[name[0]][element] = self.value_to_dict(name[1:], value, _dict[name[0]][element])
            else:
                _dict[name[0]] = [self.value_to_dict(name[1:], value, {})]
    
        elif len(name) == 1:
            _dict[name[0]] = value
            
        else:
            if not _dict.has_key(name[0]):
                _dict[name[0]] = self.value_to_dict(name[1:], value, {})
            else:
                _dict[name[0]] = self.value_to_dict(name[1:], value, _dict[name[0]])
                 
        return _dict
        
    ################################################################################################################
    def cassandra_to_dict(self, data):
        _dict = {}
        for key in data.keys():
            name = key.split(".")
            _dict = self.value_to_dict(name, data[key], _dict)
        return _dict
    
    
    ################################################################################################################
    def dict_to_value(self, _dict, _slot, _type):
        
        if isinstance(_slot, str):
            return _dict
        elif isinstance(_slot, int):
            return int(_dict[_slot])
        elif isinstance(_slot, float):
            return float(_dict)
        elif isinstance(_slot, bool):
            return bool(_dict)
        elif isinstance(_slot, (list, dict, tuple)):
            msg = []
            _type = _type.replace("[]","")
            _slot = genpy.message.get_message_class(_type)()
            for elem in _dict:
                msg.append( self.dict_to_ros( elem, _slot) )
            return msg
        elif isinstance(_slot, (genpy.rostime.Time, genpy.rostime.Duration)):
            t = rospy.Time()
            t.set(int(_dict['secs']), int(_dict['nsecs']))
            return t
        elif isinstance(_slot, rospy.Message):
            return self.dict_to_ros(_dict, _slot)
        else:
            raise RosCassandraTranspileException("unknown type: %s"%type(_slot)) 
        
    def dict_to_ros(self, _dict, msg):
        
        for i in range(len(msg.__slots__)):
            _slot = msg.__slots__[i]
            _type = msg._slot_types[i]
            
            setattr(msg, 
                    _slot,
                    self.dict_to_value( _dict[_slot],
                                        getattr(msg, _slot),
                                        _type))
    
        return msg
