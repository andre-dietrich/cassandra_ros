#!/usr/bin/env python
# -*- coding: utf-8 -*-
import roslib; roslib.load_manifest("cassandra_ros")
import rospy
import genpy

from CassandraTopic_        import *
from CassandraTopic_binary  import *
from CassandraTopic_ros     import *
from CassandraTopic_string  import *
from CassandraTopic_yaml    import *


import rostopic

import std_msgs.msg
import rosmsg

import pycassa
import time

import re

import cql

KeyFormats = ['time', 'hash', 'msg_part', 'manual']
CassandraFormats = ['binary', 'string', 'ros', 'ros_old', 'yaml', 'manual']

class CassandraTopicException(rospy.ROSException): pass

class CassandraTopic(CassandraTopic_): #, CassandraTopic_ros, CassandraTopic_string):
    
    def __init__(self, topic="",
                 column=None,               # pycassa Column
                 cursor=None,               # cql cursor
                 cassandra_format=CassandraFormats[0],     # binary, string, ros...
                 msg_class='',            # tfMessage, uint8, etc
                 msg_package='',          # the package where the msg-format was defined
                 key_format=KeyFormats[0],  # Timestamp, hash of msg or a part of the msg, like a sequence 
                 key_msg_part='',         # if a part of the message was defined ... wich one
                 comment = "",              # add a comment to topic
                 date=None,
                 meta=None):                # add a date ...
        
        if meta:
            self.setMeta(meta)
            
        else:
            self.topic              = topic
            self.cassandra_format   = cassandra_format
        
            self.msg_class          = msg_class
            self.msg_package        = msg_package
            self.key_format         = key_format
            self.key_msg_part       = key_msg_part
            self.comment            = comment
            self.date               = date
          
            if not date:
                self.date = time.asctime()
            
            self._generateMsg()
        
        self.column = column
        
        # change the baseclass verry importate ,)
        self.__class__.__bases__ = (eval("CassandraTopic_"+self.cassandra_format) ,)
        #CassandraTopic_.__init__(self, self.MsgClass)
        
        self.cursor=cursor
            
    def getMeta(self):
        meta = {}
        
        meta['topic']               = self.topic
        meta['cassandra_format']    = self.cassandra_format
        
        meta['msg_class']           = self.msg_class
        meta['msg_package']         = self.msg_package
        meta['key_format']          = self.key_format
        meta['key_msg_part']        = self.key_msg_part
        meta['comment']             = self.comment
        meta['date']                = self.date
        
        return meta
    
    def setMeta(self, meta):
        self.topic              = meta['topic']
        self.cassandra_format   = meta['cassandra_format']
        
        self.msg_class          = meta['msg_class']
        self.msg_package        = meta['msg_package']
        self.key_format         = meta['key_format']
        self.key_msg_part       = meta['key_msg_part']
        self.comment            = meta['comment']
        self.date               = meta['date']
        
        self._generateMsg()
        
    def _generateMsg(self):
        if (len(self.msg_class)==0 or len(self.msg_package))==0 and len(self.topic)>0:
            msg_class, _, _ = rostopic.get_topic_class(self.topic, blocking=False)
            self.msg_package = msg_class.__module__.split(".")[0]
            self.msg_class   = msg_class.__name__
        
        self.MsgClass           = genpy.message.get_message_class(self.msg_package+"/"+self.msg_class)
        
        if not self.MsgClass:
            roslib.launcher.load_manifest(self.msg_package)
            self.MsgClass = genpy.message.get_message_class(self.msg_package+"/"+self.msg_class)
            if not self.MsgClass:
                raise CassandraTopicException("failed to initialize ros msg of type: "+self.msg_package+"/"+self.msg_class)
    
    def addData(self, msg, key=None, ttl=None):
        # add a list of messages
        if isinstance(msg, list):
            # keys are already defined
            if isinstance(key, list):
                for i in len(msg):
                    self.column.insert(key[i], self.encode(msg[i]), ttl=ttl)
            else:
                for i in len(msg):
                    self.column.insert(self.createKey(msg[i]), self.encode(msg[i]), ttl=ttl)
        else:
            if not key:
                key = self.createKey(msg)
                
            self.column.insert(key, self.encode(msg), ttl=ttl) 
    
    def getData(self, key, to_key=None, queue=100):
        if isinstance(key, list):
            data = []
            for k in key:
                data.append(self.getData(k))
        # a range of keys is defined
        elif to_key:
            data = []
            values = self.column.get_range(start=key, finish=to_key, row_count=queue, include_timestamp=False)
            
            for value in values:
                key, value = value
                data.append((key, self.decode(value)))
        #return a single value
        else:
            value = self.column.get(key, include_timestamp=False)
            msg = self.decode(value)
            data = (key,msg)
            
        return data
    
    def getAllData(self, queue=100):
        data = []
        iter_ = self.column.get_range(start='', row_count=queue, include_timestamp=False)
        for element in iter_:
            key, value = element
            data.append((key, self.decode(value)))
        return data
        
    def removeData(self, key, to_key=None, queue=100):
        # remove a list of keys
        if isinstance(key, list):
            for k in key:
                self.column.remove(k)
        
        # remove a range of values
        elif to_key:
            while True:
                values = list(self.column.get_range(start=key, finish=to_key, row_count=100))
                
                for value in values:
                    _key, _ = value
                    self.column.remove(_key)
                
                if len(values) < queue:
                    break
                
        # remove a single value
        else:
            self.column.remove(key) 
            
    def removeAllData(self):
        while True:
            values = list(self.column.get_range(start='', row_count=100))
            
            for value in values:
                _key, _ = value
                self.column.remove(_key)
                
            if len(values) == 0:
                break
    
    def createKey(self, msg):
        if self.key_format == KeyFormats[0]: # time
            _time = rospy.Time.now()
            return str(_time.to_nsec())
        elif self.key_format == KeyFormats[1]: # hash
            pass
        elif self.key_format == KeyFormats[2]: # msg_part
            return str(eval(self.key_msg_part))
        else:
            raise CassandraTopicException("unknown keyformat: "+str(self.key_format))
        
    def countData(self):
        self.cursor.execute("select count(*) from '"+self.column.column_family+"'")
        return self.cursor.fetchone()[0]
        