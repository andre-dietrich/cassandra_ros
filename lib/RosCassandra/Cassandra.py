#!/usr/bin/env python
# -*- coding: utf-8 -*-
import roslib; roslib.load_manifest("cassandra_ros")
import rospy
import pycassa

class Cassandra():
    def __init__(self, host="localhost", port=9160):
        self.host       = host
        self.port       = port
                
        self.sysManager = pycassa.SystemManager(str(self.host)+":"+str(self.port), framed_transport=True, timeout=30)
        
    def createKeyspace(self, keyspace=False,
                               replication_strategy='SimpleStrategy',
                               strategy_options={'replication_factor': '1'}):
        try:
            if keyspace == False:
                keyspace = self.keyspace
            self.sysManager.create_keyspace(keyspace, replication_strategy, strategy_options)
        except:
            return False
        return True
    
    def dropKeyspace(self, keyspace=False):
        try:
            if keyspace == False:
                keyspace = self.keyspace
            self.sysManager.drop_keyspace(keyspace)
        except:
            return False
        return True
    
    def existKeyspace(self, keyspace=False):
        try:
            if keyspace == False:
                keyspace = self.keyspace
            keyspaces = self.sysManager.list_keyspaces()
            return any(keyspace == item for item in keyspaces)
        except:
            pass
        return False
    
    def getColumnComment(self, col_name, keyspace=False):
        try:
            if keyspace == False:
                keyspace = self.keyspace
            columns = self.sysManager.get_keyspace_column_families(keyspace)
            return columns[col_name].comment
        except:
            pass
        return False
    
    def getAllColumns(self, keyspace=False):
        try:
            if keyspace == False:
                keyspace = self.keyspace
            columns = self.sysManager.get_keyspace_column_families(keyspace)
            return columns.keys()
        except:
            pass
        return False
    
    def setColumnComment(self, col_name, comment, keyspace=False):
        try:
            if keyspace == False:
                keyspace = self.keyspace
            self.sysManager.alter_column_family(keyspace, col_name, comment=comment)
            return True
        except:
            pass
        return False
        
    def createColumn(self, col_name, super=False, column_validation_classes=None):#,compression_options={'sstable_compression':'SnappyCompressor','chunk_length_kb':'64'}):
        try:
            self.sysManager.create_column_family(self.keyspace,
                                                 col_name,
                                                 super=super,
                                                 #default_validation_class=pycassa.BYTES_TYPE,
                                                 comparator_type=pycassa.UTF8_TYPE,
                                                 column_validation_classes=column_validation_classes,
                                                 key_validation_class=pycassa.UTF8_TYPE)#,
                                                 #compression_options=compression_options)
        except:
            return False
        return True
    
    def dropColumn(self, col_name):
        try:
            self.sysManager.drop_column_family(self.keyspace, col_name)
        except:
            return False
        return True
    
    def existColumn(self, col_name, keyspace=False):
        try:
            if keyspace == False:
                keyspace = self.keyspace
            colums = self.sysManager.get_keyspace_column_families(keyspace)
            if colums.has_key(col_name):
                return True
        except:
            pass
        return False
    
    def typeOfColumn(self, col_name, keyspace=False):
        try:
            if keyspace == False:
                keyspace = self.keyspace
            colums = self.sysManager.get_keyspace_column_families(keyspace)
            return colums[col_name].column_type
        except:
            pass
        return False
    
    def getColumn(self, col_name):
        try:
            return pycassa.ColumnFamily(self.pool, col_name)
        except:
            pass
        return False
    
    def connectToKeyspace(self, keyspace="ros"):
        self.keyspace   = keyspace
        try:
            self.pool = pycassa.ConnectionPool(self.keyspace, [self.host + ":" + str(self.port)])
        except:
            return False
        return True
        
    def disconnect(self):
        try:
            self.pool.dispose()
        except:
            return False
        return True
        