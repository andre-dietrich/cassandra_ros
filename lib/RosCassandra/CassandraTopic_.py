import roslib; roslib.load_manifest("cassandra_ros")
import rospy
import genpy

class CassandraTopic_(object):
    def __init__(self, MsgClass):
        self.MsgClass = MsgClass

    def getColumnValidationClasses(self):
        return {}

    def encode(self, msg):
        pass

    def decode(self, data):
        pass
