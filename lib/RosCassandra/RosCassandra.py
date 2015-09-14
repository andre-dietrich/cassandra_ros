#!/usr/bin/env python
# -*- coding: utf-8 -*-
import roslib; roslib.load_manifest("cassandra_ros")
import roslib.message
import rospy
import rosmsg
import rostopic
#import rosjson
import genpy.dynamic

import rosbag
import simplejson as json
import hashlib
import time
import StringIO

import cql
import pycassa

from Cassandra import Cassandra
from CassandraTopic import CassandraTopic

class RosCassandraException(rospy.ROSException): pass

class RosCassandra(Cassandra):
    
    def __init__(self, host="localhost", port=9160, keyspace="ros"):
        Cassandra.__init__(self, host, port)
        
        self.cql_conn   = None
        self.cql_cursor = None        
    
    def addTopic(self, topic, cassandra_format=None,     # binary, json-string, ros...
                               msg_class=None,            # tfMessage, uint8, etc
                               msg_package=None,          # the package where the msg-format was defined
                               key_format=None,           # Timestamp, hash of msg or a part of the msg, like a sequence 
                               key_msg_part=None,         # if a part of the message was defined ... wich one
                               comment = "",              # add a comment to topic
                               date=None):
        
        if self.existTopic(topic):
            return False
        
        topic_md5 = self.topic2Hash(topic)
        
        #column_validation_classes={'data':pycassa.BYTES_TYPE,
        #                           'format':pycassa.UTF8_TYPE,
        #                           'header.frame_id':pycassa.UTF8_TYPE,
        #                           'header.seq':pycassa.INT_TYPE,
        #                           'header.stamp.nsecs':pycassa.INT_TYPE,
        #                           'header.stamp.secs':pycassa.INT_TYPE}
        
        casTopic = CassandraTopic(topic, None, None,
                                  cassandra_format,
                                  msg_class, msg_package,
                                  key_format, key_msg_part,
                                  comment, date)
        
        column_validation_classes = casTopic.getColumnValidationClasses()
               
        column = self.createColumn(topic_md5, column_validation_classes=column_validation_classes)
        
        meta = casTopic.getMeta()
        self.setTopicMeta(topic, meta)
        
    def topic2Hash(self, topic):
        return hashlib.md5(topic).hexdigest()
        
    def getTopic(self, topic):
        
        if not self.existTopic(topic):
            return False        
        
        topic_md5 = self.topic2Hash(topic)
        column = self.getColumn(topic_md5)
        
        meta = self.getTopicMeta(topic)
        
        return CassandraTopic(column=column, cursor=None, meta=meta)
    
    def removeTopic(self, topic):
        if self.existTopic(topic):
            topic_md5 = self.topic2Hash(topic)
            self.dropColumn(topic_md5)
        
    def getTopicMeta(self, topic=None, topic_md5=None):
        if topic:
            topic_md5 = self.topic2Hash(topic)
        try:
            meta = json.loads(self.getColumnComment(topic_md5))
        except:
            meta = {}
        
        return meta
    
    def setTopicMeta(self, topic, meta):
        topic_md5 = self.topic2Hash(topic)
        self.setColumnComment(topic_md5, json.dumps(meta))
        
    def existTopic(self, topic):
        topic_md5 = self.topic2Hash(topic)
        return self.existColumn(topic_md5)
    
    def getAllTopics(self):
        topics = []
        columns = self.getAllColumns()

        for col in columns:
            meta = self.getTopicMeta(topic_md5=col)
            if type(meta) == dict:
                if meta.has_key('topic'):
                    topics.append(meta['topic'])
        
        return topics
     
    def renameTopic(self, topic, topic_new):
        topic_md5 = self.topic2Hash(topic)
        meta = self.getTopicMeta(topic_md5)
        meta['topic'] = topic_new
        
        pass
    
    
    def fileExport(self, topics, filename):
        pass
    
    def fileImport(self, filename, topics=None, format='ros', key='time', key_msg_part=None):
        print "read ...",
        bag = rosbag.Bag(filename)
        print "done"
        casTopic = {}
        i = 0
        for topic, msg, t in bag.read_messages(topics):
            if not casTopic.has_key(topic):
                if not self.existTopic(topic):
                    
                    self.addTopic(topic,
                                  cassandra_format=format,
                                  msg_class = msg.__class__._type.split('/')[1],
                                  msg_package = msg.__class__._type.split('/')[0],
                                  key_format=key, 
                                  key_msg_part=key_msg_part,
                                  comment = "",
                                  date=None)
                    
                casTopic[topic] = self.getTopic(topic)

            i += 1
            print i
            #time.sleep(5)
            if key == 'time':
                casTopic[topic].addData(msg, str(t.to_nsec()))
            else:
                casTopic[topic].addData(msg)
        
    
    def countTopicData(self, topic):
        try:
            topic_md5 = self.topic2Hash(topic)
            self.cursor.execute("select count(*) from '"+topic_md5+"'")
            return self.cursor.fetchone()[0]
        except:
            return 0
    
    def exequteCQL(self, query):
        # establish the cql connection
        if self.cql_cursor == None:
            self.cql_conn = cql.connect(self.host, self.port, self.keyspace)
            self.cql_cursor = self.cql_conn.cursor()
        
        topics = self.getAllTopics()
        
        # rename topics to column names ...
        for topic in topics:
            query = query.replace('"'+topic+'"', '"'+self.topic2Hash(topic)+'"')
        print query
        
        #return query
        # execute querie
        self.cql_cursor.execute(query)
        return self.cql_cursor.fetchall()
    
    def createIndex(self, topic, column):
        indexname = topic+column
        indexname = indexname.replace(".","_")
        column_validator = self.getTopic(topic).column.column_validators[column]
        
        self.sysManager.create_index(self.keyspace,
                                     self.topic2Hash(topic), 
                                     column, 
                                     column_validator,
                                     index_name=indexname)

    def removeIndex(self, topic, column):
        self.sysManager.drop_index(self.keyspace, self.topic2Hash(topic), column)