#!/usr/bin/env python
import roslib; roslib.load_manifest('cassandra_ros')
import rospy
import rostopic

from cassandra_ros.srv import *
import RosCassandra.RosCassandra as rc

import time
import threading
from PyQt4.QtCore import QThread



def enum(**enums):
    return type('Enum', (), enums)

running = enum(START=1, STOP=0, PAUSE=-1)

class RosCassandraBag(QThread):
    MAX_GWMTIME = 4294967295
    MIN_GWMTIME = 0

    def __init__(self, casTopic, parent=None):
        QThread.__init__(self,parent)
        self.casTopic= casTopic

        self.record_start_time = self.MIN_GWMTIME
        self.record_stop_time  = self.MAX_GWMTIME
        self.record_status     = running.STOP
        self.record_filter     = None

        self.play_start_time = self.MIN_GWMTIME
        self.play_stop_time  = self.MAX_GWMTIME
        self.play_status     = running.STOP
        self.play_filter     = None

    def record(self, msg):
        # start recording

        # Andre
        #msg = msg.transforms.pop()

        if rospy.get_time() >= self.record_start_time:
            if self.record_status == running.START:
                if self.record_filter:
                    if( eval(self.record_filter) ):
                        self.casTopic.addData(msg, ttl=self.record_ttl)
                else:
                    self.casTopic.addData(msg)

        if rospy.get_time() >= self.record_stop_time:
            self.stopRecord()

    def startRecord(self, start_time=MIN_GWMTIME, stop_time=MAX_GWMTIME, filter=None, ttl=None):
        self.record_start_time = min(self.record_start_time, start_time)
        self.record_stop_time  = max(self.record_stop_time, stop_time)
        self.record_filter     = filter
        self.record_ttl        = ttl

        self.record_status     = running.START

        self.subscriber = rospy.Subscriber(self.casTopic.topic, self.casTopic.MsgClass, self.record)

    def stopRecord(self):
        if self.record_status == running.START or self.record_status == running.PAUSE:
            self.record_status = running.STOP
            self.subscriber.unregister()

    def pauseRecord(self):
        if self.record_status == running.START:
            self.record_status = running.PAUSE
        elif self.record_status == running.PAUSE:
            self.record_status = running.START

    def exit(self):
        self.stopPlay()
        self.stopRecord()

    def startPlay(self, start_time=MIN_GWMTIME, stop_time=MAX_GWMTIME, speed=1, delay=0, queuesize=100, loop=False, filter=None):
        self.play_start_time    = start_time
        self.play_stop_time     = stop_time
        self.play_speed         = speed
        self.play_delay         = delay
        self.play_queuesize     = queuesize
        self.play_loop          = loop
        self.play_filter        = filter

        self.play_status     = running.START

        if not self.play_filter or self.play_filter == "":
            self.play_filter = None

        self.publisher = rospy.Publisher(self.casTopic.topic, self.casTopic.MsgClass)
        # start thread
        self.start()

    def stopPlay(self):
        if self.isRunning():
            self.terminate()


    def pausePlay(self):
        # waiting
        if self.isRunning():
            if self.play_status == running.START:
                self.play_status = running.PAUSE
            elif self.play_status == running.PAUSE:
                self.play_status = running.START

    # play ...
    def run(self):
        rospy.sleep(self.play_delay)

        while True:

            from_key = str(self.play_start_time)
            to_key   = str(self.play_stop_time)

            _last_time   = long(from_key)
            _currentTime = rospy.Time.now().to_nsec()

            while True:
                data = self.casTopic.getData(from_key, to_key, self.play_queuesize)

                for dat in data:
                    from_key, msg = dat

                    timestamp = long(from_key)

                    if _last_time > 0:
                        delta_t = float(timestamp - _last_time) / (self.play_speed*1000000000)
                    else:
                        delta_t = 0

                    time.sleep(delta_t)

                    _last_time = timestamp

                    if self.play_filter:
                        if eval(self.play_filter):
                            self.publisher.publish(msg)
                    else:
                        self.publisher.publish(msg)

                    # pause
                    while self.play_status == running.PAUSE:
                        self.yieldCurrentThread()


                from_key = str(long(from_key)+1)

                # end reached
                if len(data) < self.play_queuesize:
                    break

            if not self.play_loop:
                break

        self.play_status     = running.STOP
        rospy.loginfo("STOP")


def handle_record(req):
    global rosCas, bag
    response = ""

    if req.ttl == 0:
            req.ttl = None

    for topic in (req.topics):
        # start recording
        if req.record==1:
            if not bag.has_key(topic):
                if not rosCas.existTopic(topic):



                    msg_class, _, _ = rostopic.get_topic_class(topic, blocking=True)

                    rosCas.addTopic(topic,
                                    req.cassandra_format,
                                    msg_class.__name__,
                                    msg_class.__module__.split(".")[0],
                                    'time', None,
                                    comment='')


                bag[topic] =  RosCassandraBag(rosCas.getTopic(topic))

            bag[topic].startRecord(req.start_time, req.stop_time, req.filter, req.ttl)
            rospy.loginfo("start recording: "+topic)

        # stop recording
        elif req.record == 0:
            if bag.has_key(topic):
                bag[topic].stopRecord()
                rospy.loginfo("stop recording: "+topic)

        # pause recording
        else:
            if bag.has_key(topic):
                bag[topic].pauseRecord()
                rospy.loginfo("pause recording: "+topic)

    return response

def handle_play(req):
    global rosCas, bag
    response = ""
    for topic in (req.topics):
        # start playing
        if req.play == 1:
            if not bag.has_key(topic):
                if rosCas.existTopic(topic):
                    bag[topic] =  RosCassandraBag(rosCas.getTopic(topic))
                    bag[topic].startPlay(start_time = req.start_time,
                                         stop_time  = req.stop_time,
                                         speed      = req.speed,
                                         delay      = req.delay,
                                         queuesize  = req.queuesize,
                                         loop       = req.loop,
                                         filter     = req.filter)
                else:
                    rospy.loginfo("topic ("+topic+") does not exist: ")


            else:
                bag[topic].startPlay(start_time = req.start_time,
                                     stop_time  = req.stop_time,
                                     speed      = req.speed,
                                     delay      = req.delay,
                                     queuesize  = req.queuesize,
                                     loop       = req.loop,
                                     filter     = req.filter)

            rospy.loginfo("start playing: "+topic)
        # stop playing
        elif req.play == 0:
            if bag.has_key(topic):
                bag[topic].stopPlay()
                rospy.loginfo("stop playing: "+topic)

        # pause playing
        elif req.play == -1:
            if bag.has_key(topic):
                bag[topic].pausePlay()
                rospy.loginfo("pause playing: "+topic)

    return response

def handle_delete(req):
    global rosCas, bag
    response = ""
    for topic in (req.topics):
        # start delete
        if rosCas.existTopic(topic):
            _topic = rosCas.getTopic(topic)
            rospy.loginfo("deleting "+topic)
            _topic.removeData(key=str(req.start_time), to_key=str(req.stop_time))
        else:
            rospy.loginfo("deleting failed, topic ("+topic+") does not exist")
    return response

def handle_truncate(req):
    global rosCas, bag
    response = ""
    for topic in (req.topics):
        # start delete
        if rosCas.existTopic(topic):
            rosCas.removeTopic(topic)
            rospy.loginfo("truncate "+topic)
        else:
            rospy.loginfo("truncate failed, topic ("+topic+") does not exist")
    return response

def handle_info(req):
    global rosCas, bag
    response = "\n"
    print req.command
    # return all available topics
    if req.command == 'list':
        topics = rosCas.getAllTopics()
        response += "number of topics stored in CassandraDB: "+str(len(topics)) +"\n"
        for i in range(len(topics)):
            response += str(i+1)+". "+topics[i]+" ("+str(rosCas.countTopicData(topics[i]))+")"+"\n"

    elif req.command == 'status':
        response += "list of connected hosts: "+str(rosCas.host) +"\n"
        for topic in bag.keys():
            if bag[topic].play_status == running.START:
                response += topic + ": playback is running\n"
            elif bag[topic].play_status == running.PAUSE:
                response += topic + ": playback is paused\n"
            elif bag[topic].record_status == running.START:
                response += topic + ": recording is running\n"
            elif bag[topic].record_status == running.PAUSE:
                response += topic + ": recording is paused\n"
            else:
                response += topic + ": is idle\n"

    elif req.command == 'info':
        for topic in req.topics:
            meta = rosCas.getTopicMeta(topic)
            for key in meta.keys():
                response += key+": "+str(meta[key])+"\n"
            response += "column name: "+rosCas.topic2Hash(topic)+"\n"
            response += "number of entries: "+str(rosCas.countTopicData(topic))+"\n"

    elif req.command == 'cql':
        print rosCas.exequteCQL(req.topics[0])

    else:
        rsp += "unknown command: "+req.command+"\n"

    return response

if __name__ == "__main__":
    host =      rospy.get_param('/cassandraBag/host', "localhost")
    port =      rospy.get_param('/cassandraBag/port', 9160)
    keyspace =  rospy.get_param('/cassandraBag/keyspace', "test")

    rosCas = rc.RosCassandra(host, port)
    rospy.loginfo("connected to Cassandra on %s:%d"%(host,port))

#    rosCas.dropKeyspace(keyspace)
    if not rosCas.connectToKeyspace(keyspace):
        rosCas.createKeyspace(keyspace)

    rosCas.connectToKeyspace(keyspace)
    rospy.loginfo("connected to Keyspace \"%s\""%(keyspace))

    bag = {}

    rospy.init_node('cassandraBag')
    service = {}
    service['record']   = rospy.Service('cassandra_record',     record,     handle_record)
    service['play']     = rospy.Service('cassandra_play',       play,       handle_play)
    service['delete']   = rospy.Service('cassandra_delete',     delete,     handle_delete)
    service['info']     = rospy.Service('cassandra_info',       info,       handle_info)
    service['truncate'] = rospy.Service('cassandra_truncate',   truncate,   handle_truncate)

    rospy.loginfo("start listening ... ")
    rospy.spin()

    for _bag in bag.itervalues():
        _bag.exit()

    rosCas.disconnect()
