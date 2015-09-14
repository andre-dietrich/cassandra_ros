import roslib; roslib.load_manifest("cassandra_ros")
import rospy
import std_msgs.msg
import rosmsg
import genpy

import pickle
import pycassa

from CassandraTopic_ import *

class CassandraTopic_ros(CassandraTopic_):
    def __init__(self, MsgClass):        
        CassandraTopic_.__init__(MsgClass)
    
    def getColumnValidationClasses(self):
        msg = self.MsgClass()
        
        spec = rosmsg.get_msg_text(msg._type)
        spec = spec.split('\n')
        spec.remove("")
        spec, _ = self._parseMsg(spec)
        
        spec = self._translateSpecToCassandra(spec)        
        
        
        
        return spec
    
    def _translateSpecToCassandra(self, spec):
        for key in spec.keys():
            if spec[key] in ('int8', 'int16', 'int32', 'uint8','uint16', 'uint32'):
                spec[key] = pycassa.INT_TYPE
            elif spec[key] in ('int64', 'uint64'):
                spec[key] = pycassa.LONG_TYPE
            elif spec[key] in ('float32'):
                spec[key] = pycassa.FLOAT_TYPE
            elif spec[key] in ('float64'):
                spec[key] = pycassa.DOUBLE_TYPE
            elif spec[key] in ('char', 'string', 'char[]'):
                spec[key] = pycassa.UTF8_TYPE
            elif spec[key] in ('time', 'duration'):
                spec[key] = pycassa.DATE_TYPE
            elif spec[key] in ('bool'):
                spec[key] = pycassa.BOOLEAN_TYPE
            else:
                spec[key] = pycassa.BYTES_TYPE
        return spec
    
    def _parseMsg(self, defs, parent=""):
        _dict = {} 
        parent += "."
        i = 0
        while i< len(defs):
            
            _type, _name = defs[i].replace('  ','').split(" ")
            
            # last one ...
            if len(defs)-1 == i:
                _dict[parent+_name] = _type
            elif defs[i].count(" ") == defs[i+1].count(" "):
                _dict[parent+_name] = _type
            elif defs[i].count(" ") > defs[i+1].count(" "):
                _dict[parent+_name] = _type
                break
            else:
                # its an array
                if _type.endswith("[]"):
                    _name += "[]"
                _d, _i = self._parseMsg(defs[i+1:], parent+_name)
                
                i += _i+1
                for key in _d.keys():
                    _dict[key] = _d[key]
            
            i += 1
                
        return _dict, i
                
    def encode(self, msg):
        return self._ros_to_cassandra(msg)
    
    def decode(self, data):
        msg = self.MsgClass()
        _dict = self._cassandra_to_dict(data)
        msg = self._dict_to_ros(_dict, msg)
        return msg
    
    def _value_to_cassandra(self ,data, parent):

        if type(data) in ( str, int, float, long, bool ):
            return { parent : data }
        elif isinstance(data, (genpy.rostime.Time, genpy.rostime.Duration)):
            return { parent : data.to_time() }#{ parent+'.secs':data.secs, parent+'.nsecs':data.nsecs}
        elif type(data) in (list, tuple):
            if len(data) == 0:
                return { parent: pickle.dumps(data) }
            elif type(data[0]) in ( str, int, float, long, bool ):
                return { parent: pickle.dumps(data) }
            else:
                _dict = {}
                for i in range(len(data)):
                    res = self._value_to_cassandra(data[i], parent)#+"["+str(i)+"]")
                    for key in res.keys():
                        _dict[key] = res[key]
                return _dict
        elif isinstance(data, rospy.Message):
            return self._ros_to_cassandra(data, parent)
        else:
            raise CassandraTopicException("unknown type: %s"%type(data)) 
    
    ################################################################################################################    
    def _ros_to_cassandra(self, data, parent=""):
        if not isinstance(data, rospy.Message):
            #raise RosCassandraTranspileException("not a valid rospy Message instance: %s"%data.__class__.__name__)
            return self._value_to_cassandra(data, parent)
    
        _dict = {}
    
        parent += "."        
        
        for elem in data.__slots__:
            sub_elements = self._value_to_cassandra( getattr(data, elem) , parent+elem)
            
            if type(sub_elements) == dict:
                for key in sub_elements.keys():
                    _dict[key] = sub_elements[key]
                
        return _dict
    
    ################################################################################################################
    
    def _value_to_dict(self, name, value, _dict):

        # treat as array
        if name[0][-1] == ']':
            ar = name[0].find('[')
            element = int(name[0][ar+1:-1])
            name[0] = name[0][:ar]
            
            # it is already an array
            if _dict.has_key(name[0]):
                if len(_dict[name[0]]) == element:
                    _dict[name[0]].append(self._value_to_dict(name[1:], value, {}))
                else:
                    _dict[name[0]][element] = self._value_to_dict(name[1:], value, _dict[name[0]][element])
            else:
                _dict[name[0]] = [self._value_to_dict(name[1:], value, {})]
    
        elif len(name) == 1:
            _dict[name[0]] = value
            
        else:
            if not _dict.has_key(name[0]):
                _dict[name[0]] = self._value_to_dict(name[1:], value, {})
            else:
                _dict[name[0]] = self._value_to_dict(name[1:], value, _dict[name[0]])
                 
        return _dict
        
    def _cassandra_to_dict(self, data):
        _dict = {}
        
        for key in data.keys():
            name = key.split(".")
            _dict = self._value_to_dict(name[1:], data[key], _dict)
        return _dict
    
    def _dict_to_value(self, _dict, _slot, _type):
        
        if isinstance(_slot, (str, int, float, bool)):
            return _dict
        elif isinstance(_slot, (list, dict, tuple)):
            msg = []
            
            if _type in ('bool[]', 'int8[]', 'uint8[]', 'int16[]', 'uint16[]', 'int32[]', 'uint32[]',
                         'int64[]', 'uint64[]', 'float32[]', 'float64[]', 'string[]'):
                msg = pickle.loads(_dict)
            else:
                _type = _type.replace("[]","")
                _slot = genpy.message.get_message_class(_type)()
                for elem in _dict:
                    msg.append( self._dict_to_ros( elem, _slot) )
            return msg
        elif isinstance(_slot, (genpy.rostime.Time, genpy.rostime.Duration)):
            t = rospy.Time()
            t.set(int(_dict.strftime('%s')), _dict.microsecond)
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
