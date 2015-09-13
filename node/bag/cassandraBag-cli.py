#!/usr/bin/env python
import roslib; roslib.load_manifest('cassandra_ros')
import rospy
from CassandraTopic import CassandraFormats
from CassandraTopic_ import *
import cassandra_ros.srv

import sys
import argparse

def call_record(args):
    if args.start=='start':
        args.start=1
    elif args.start=='stop':
        args.start=0
    else: # pause
        args.start=-1
    rospy.wait_for_service('/cassandra_record')
    try:
        call = rospy.ServiceProxy('/cassandra_record', cassandra_ros.srv.record)
        
        infos = call(record             = args.start,
                     topics             = args.topics,
                     cassandra_format   = args.cassandra_format,
                     filter             = args.filter,
                     start_time         = args.start_time,
                     stop_time          = args.stop_time,
                     ttl                = args.ttl,
                     apply              = args.apply )
        
        print infos
    except rospy.ServiceException, e:
        print "Service call failed: %s"%e

def call_play(args):
    if args.start=='start':
        args.start=1
    elif args.start=='stop':
        args.start=0
    else: # pause
        args.start=-1

    rospy.wait_for_service('/cassandra_play')
    try:
        call = rospy.ServiceProxy('/cassandra_play', cassandra_ros.srv.play)
        infos = call(play       = args.start,
                     topics     = args.topics,
                     start_time = args.start_time,
                     stop_time  = args.stop_time,
                     filter     = args.filter,
                     queuesize  = args.queue,
                     delay      = args.delay,
                     speed      = args.speed,
                     loop       = args.loop)
        
        print infos
    except rospy.ServiceException, e:
        print "Service call failed: %s"%e
   
def call_delete(args):
    rospy.wait_for_service('/cassandra_delete')
    try:
        call = rospy.ServiceProxy('/cassandra_delete', cassandra_ros.srv.delete)
        infos = call(topics     = args.topics,
                     start_time = args.start_time,
                     stop_time  = args.stop_time)
        print infos
    except rospy.ServiceException, e:
        print "Service call failed: %s"%e
        
def call_truncate(args):
    rospy.wait_for_service('/cassandra_truncate')
    try:
        call = rospy.ServiceProxy('/cassandra_truncate', cassandra_ros.srv.truncate)
        info = call(topics = args.topics)
        print info
    except rospy.ServiceException, e:
        print "Service call failed: %s"%e
        
def call_info(args):
    rospy.wait_for_service('/cassandra_info')
    try:
        call = rospy.ServiceProxy('/cassandra_info', cassandra_ros.srv.info)
        info = call(args.command, args.topics)
        print info
    except rospy.ServiceException, e:
        print "Service call failed: %s"%e


if __name__ == "__main__":
    cl_parser = argparse.ArgumentParser()
    cl_subparser = cl_parser.add_subparsers()
    
    ################################################################################################################################
    record = cl_subparser.add_parser('record')
    record.set_defaults(func=call_record)
    record.add_argument(dest="start", help="start|stop|pause recording", choices=['start','stop', 'pause'])
    record.add_argument("-f", "--format", dest="cassandra_format", help="store date in database either add "+str(CassandraFormats), choices=CassandraFormats, default=CassandraFormats[0])
    record.add_argument("-x", "--filter", dest="filter", help="write filter in python style, starting with (msg.header.seq%10==0)", default='')
    record.add_argument("-b", "--begin", dest="start_time", help="Start record at given time as int", type=int, default=0)
    record.add_argument("-e", "--end", dest="stop_time", help="Stop record at given time as int", type=int,  default=4294967295)
    record.add_argument("-ttl", "--timetolive", dest="ttl", help="Stop record at given time as int", type=int,  default=None)
    record.add_argument("-a",  "--apply", dest="apply", help="apply some functions like msg.transforms.pop()", default=None)
    record.add_argument(metavar='TOPICS', nargs='+', dest="topics", help="Topics you want to record. Example: /turtle1/command_velocity")
    
    ################################################################################################################################
    play = cl_subparser.add_parser('play')
    play.set_defaults(func=call_play)
    play.add_argument(dest="start", help="start|stop|pause playback", choices=['start','pause','stop'])
    play.add_argument("-x", "--filter", dest="filter", help="write filter in python style, starting with (msg.header.seq%10==0)", default='')
    play.add_argument("-s", "--speed", dest="speed", help="Playback speed as float. Default: 1.0", type=float, default=1.0)
    play.add_argument("-q", "--queue", dest="queue", help="Queue", type=int, default=100)
    play.add_argument("-d", "--delay", dest="delay", help="Delay in seconds", type=int, default=0)
    play.add_argument("-l", "--loop",  dest="loop", help="Loop playback", action='store_true')
    play.add_argument("-b", "--begin", dest="start_time", help="Start play at given time as int", type=int, default=0)
    play.add_argument("-e", "--end",   dest="stop_time", help="Stop play at given time as int", type=int,  default=4294967295)
    play.add_argument(metavar='TOPICS', nargs='+', dest="topics", help="Topics you want to play. Example: /turtle1/command_velocity")
    
    ################################################################################################################################
    delete = cl_subparser.add_parser('delete')
    delete.set_defaults(func=call_delete)
    delete.add_argument("-b", "--begin", dest="start_time", help="Start record/play at given time as int", type=int, default=1)
    delete.add_argument("-e", "--end", dest="stop_time", help="Stop record/play at given time as int", type=int,  default=4294967295)
    delete.add_argument(metavar='TOPICS', nargs='+', dest="topics", help="Topics you want to delete. Example: /turtle1/command_velocity")
    
    ################################################################################################################################
    truncate = cl_subparser.add_parser('truncate')
    truncate.set_defaults(func=call_truncate)
    truncate.add_argument(metavar='TOPICS', nargs='+', dest="topics", help="Topics you want to delete totally. Example: /turtle1/command_velocity")
    
    ################################################################################################################################
    list = cl_subparser.add_parser('list')
    list.set_defaults(func=call_info)
    list.set_defaults(command="list")
    list.set_defaults(topics='')
    
    status = cl_subparser.add_parser('status')
    status.set_defaults(func=call_info)
    status.set_defaults(command="status")
    status.set_defaults(topics='')
    
    info = cl_subparser.add_parser('info')
    info.set_defaults(func=call_info)
    info.set_defaults(command="info")
    info.add_argument(metavar='TOPICS', nargs='+', dest="topics", help="Topics you want to have further information.")
    
    info = cl_subparser.add_parser('cql')
    info.set_defaults(func=call_info)
    info.set_defaults(command="cql")
    info.add_argument(metavar='TOPICS', nargs='+', dest="topics", help="string")
    
    ################################################################################################################################
    args = cl_parser.parse_args(sys.argv[1:])
    
    args.func(args)