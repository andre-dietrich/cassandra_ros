# cassandra_ros

Library and tools for dynamically storing ROS messages in Apache Cassandra.

|||
|--|--|
| Author      | André Dietrich                                                         |
| Source      | `git clone` https://gitlab.com/andre-dietrich/cassandra_ros.git        |
| Email       | <mailto:andre.dietrich@ovgu.de>                                        |
| Publication | ROS Meets Cassandra: Data Management in Smart Environments with NoSQL  |
|             | https://github.com/andre-dietrich/cassandra_ros/blob/master/publication/ROS_Meets_Cassandra__Data_Management_in_Smart_Environments_with_NoSQL.pdf |
| Slides      | https://github.com/andre-dietrich/cassandra_ros/blob/master/publication/presentation/presentation.svg |

## Overview

This package provides an interface to the Cassandra database system, which can
be used to store any types of ROS-messages in column families, similar to
MongoDB. But in contrast to MongoDB you are able to choose the format you would
like to store your messages. You can choose between various formats and thus,
explore your data afterwards by using Cassandra's CQL capabilities.

## Media

[![YouTube](http://img.youtube.com/vi/lQczBtVmomc/0.jpg)](http://www.youtube.com/watch?v=lQczBtVmomc "click to watch")

[![YouTube](http://img.youtube.com/vi/tfczj1jb3B4/0.jpg)](http://www.youtube.com/watch?v=tfczj1jb3B4 "click to watch")

[![YouTube](http://img.youtube.com/vi/czLQ-yxBYC4/0.jpg)](http://www.youtube.com/watch?v=czLQ-yxBYC4 "click to watch")

[![YouTube](http://img.youtube.com/vi/y6LqLNB4VDk/0.jpg)](http://www.youtube.com/watch?v=y6LqLNB4VDk "click to watch")

## Installation

First of all you need to download and install CassandraDB manually form apache:

http://cassandra.apache.org/download/

Then change the Cassandra partitioner to "ByteOrdered", which can be set in

cassandra-install/conf/cassandra.yaml

search for section partitioner and change it to the following:

partitioner: org.apache.cassandra.dht.ByteOrderedPartitioner

ByteOrdered partitioner is required to retrieve requested data ordered,
otherwise it will appear. For more information, have look on the documentation
on http://docs.datastax.com/en/cassandra/2.0/cassandra/architecture/architecturePartitionerAbout_c.html

Secondly, you will have to install pycassa, an python interface for CassandraDB.
The project is hosted on:

pycassa.github.com/pycassa/

All information including installation, documentation, or a tutorial, are there
available.

## First run

First of all you will need start Cassandra, what is done in most cases by going
into the folder, where Cassandra was installed and than by typing in:

``` bash
$ bin/cassandra -f
```
The option -f hinders Cassandra to start a deamon in backgroud, for more
information and help visit : http://wiki.apache.org/cassandra/GettingStarted

We added three simple launch-files, with which you can test, if your
installation was successful. You will need a webcam to run these examples,
camera-settings can be changed in the launch-file. First of all run

``` bash
$ roslaunch cassandra_ros recordCamera.launch
```
a window with the camera-stream should appear, furthermore this starts
cassandraBag.py, a service which stores this stream in a CassandraDB. If you
abort, recording will stop automatically. If this does not work, or you see the
message:

``` plain
Not starting RPC server as requested. Use JMX (StorageService->startRPCServer())
or nodetool (enablethrift) to start it ...
```
then simply run:

``` bash
$ sudo nodetool (enablethrift)
```

By running:

``` bash
$ roslaunch cassandra_ros replayCamera.launch
```
the previous recorded stream will be replayed. If you could see yourself,
everything seems to be correct installed and configured. By running:

``` bash
$ roslaunch cassandra_ros deleteCamera.launch
```
the column-family, in which the stream was stored, gets deleted. The service
cassandra_ros/node/bag/cassandraBag.py was in this case responsible for handling
topics, if you want to use it as a command-line tool, see section …

## Suggestions

There are a some graphical user interfaces available for Cassandra, which
simplify the interaction with Cassandra. Using these it is quite easy to check,
weather storing was successful or not, or to graphically explore your data. We
recommend the usage of the following tools:

### Cassandra Cluster Admin:

A php-tool similar to phpmyadmin and available under:
https://github.com/sebgiroux/Cassandra-Cluster-Admin

### Cassandra-Gui:

A simple java-tool, which can be downloaded from:
http://code.google.com/a/apache-extras.org/p/cassandra-gui/

### Others:

Check the following link:
http://wiki.apache.org/cassandra/Administration%20Tools

# Tutorial

## Philosophy

1. These are some design-principles we made for data handling, which might a bit
   confusing, if you examine your data with other tools or one of the
   recommended GUIs.
2. Each topic is stored within its own column-family, one message per row.
3. Due to the maximum column-family-name-length of 42 Bytes, we use the
   hash-value of the topic-name as the column-family-name. But you do not need
   to bother with hash-values, using our interface you can still work with
   topic-names, everything is hidden.
4. All meta information about a topic, including type, storage-format,
   key-format, etc. are stored within the comment-field of every column-family.
   No other column-family is required or has to be updated to store
   meta-information, what reduces the amount of additional effort, but forbids
   to change the comment-field manually. Everything is handled within the
   background.

## Basic Interface

There are only 2 classes, you need to be aware of, if you want to store and
access ros-messages within Cassandra. RosCassandra, which can be seen as the
management-interface to Cassandra in a ROS typical manner. CassandraTopic is
required to handle the access to every topic/colum-family.

## Let us start by connecting to Cassandra:

``` python
import rospy
import roslib; roslib.load_manifest("cassandra_ros")
import RosCassandra.RosCassandra as rc

rospy.init_node('CassandraTest')

# in most cases this is the standard configuration
host = 'localhost'
port = 9160

# that is nearly enough, only a keyspace is missing
rosCas = rc.RosCassandra(host, port)

# check if the keyspace exits
if not rosCas.connectToKeyspace('test'):
    # if not, create one
    rosCas.createKeyspace('test')

# connect again
rosCas.connectToKeyspace('test')
```
Thats it, now we are connected. In the next step we will create a new
topic/column-family:

``` python
# this is the datatype we will store
from std_msgs.msg import String
topic = 'test_topic'
key_format='time'       # this means, that we will use a timestamp as primary
                        # key, other formats are 'hash' or 'msg_part'
format = 'binary'       # the format that is used for conversation,
                        # other formats are 'string', 'ros', or 'yaml',

# at first we create the new topic with the following command
rosCas.addTopic(topic,
                format,
                'String',
                'std_msgs',
                key_format,
                None, # <only required if you use define key-format 'manually'
                comment='some additional comments...')

# if everything worked well, there should be a new topic and the
# following command should evaluate to true
rosCas.existTopic(topic)

# getting a list with all topics
rosCas.getAllTopics()
```
In the next step we will start to insert some messages into the database

``` python
# get the topic-containerros
topicContainer = rosCas.getTopic(topic)

# thus we can now start to insert some messages
msg = String()

for i in range(100):
    msg.data = 'test ' + str(i)
    topicContainer.addData(msg)

# because I do not know your time, we can simply get all messages by calling
topicContainer.getAllData(queue=100)

# if you know the primary keys exactly, you can also query by using them
# topicContainer.getData(key, to_key, queue=100)

# remove everything
topicContainer.removeAllData()
# or if you want to delete a special message, use:
# topicContainer.removeData(key, to_key, queue=100):

# now let us change the format for storing data and the key
# we could either delete the whole container by calling:
# rosCas.removeTopic(topic)
# or we change the meta-information … this works only, if
# the column-family is empty
meta = topicContainer.getMeta()

#meta is always a dictionary, where we change values as follows
meta['cassandra_format'] = 'ros'
meta['key_format'] = 'manual'

# write it back to the topic
rosCas.setTopicMeta(topic, meta)

# because we had changed the storage format, we have to renew the
# topic-container, this will be explained in more detail within the next
# section
topicContainer = rosCas.getTopic(topic)

for i in range(100):
    msg.data = 'test ' + str(i)
    topicContainer.addData(msg, '%.2d'%i)

# as you see, adding keys looks a bit weird, but they are always stored
# as strings... requesting data looks as follows ….
topicContainer.getData('04', '20')
# you will get a list consisting of (key, value) pairs ...

# also allowed is something like this: topicContainer.addData(msg, 'XXX'))
```

## Storage Formats

As mentioned before, we support different ways of storing data, it is either
possible to store messages in a binary format, which enables fast conversion and
storing. But currently it is also possible to store messages as "strings",
"yaml", or "ros" format. The type of conversion is defined at the creation of a
topic-container:

``` python
rosCas.addTopic(topic,
                format, # exactly in here ... 'string', 'yaml', 'ros', 'binary'
                'String',
                'std_msgs',
                key_format,
                None,
                comment)
```
If you would like to run CQL-Queries on your stored messages, you will have to
use the 'ros' – format (might not work with every datatype). Every message is
therefore translated into a linar form and column are strongly typed, according
to the defined message formats. A Message of type
"geometry_msgs/TransformStamped":

``` plain
std_msgs/Header header
   uint32 seq
   time stamp
   string frame_id
   string child_frame_id
geometry_msgs/Transform transform
   geometry_msgs/Vector3 translation
     float64 x
     float64 y
     float64 z
   geometry_msgs/Quaternion rotation
     float64 x
     float64 y
     float64 z
     float64 w
```
is then transformed into Cassandra format:

``` plain
.header.seq : INT_TYPE
.header.stamp : DATE_TYPE
.header.frame_id : UTF8_TYPE
.transform.translation.x : DOUBLE_TYPE
.transform.translation.y : DOUBLE_TYPE
.transform.translation.z : DOUBLE_TYPE
.transform.rotation.x : DOUBLE_TYPE
.transform.rotation.y : DOUBLE_TYPE
.transform.rotation.z : DOUBLE_TYPE
.transform.rotation.w : DOUBLE_TYPE
```
which allows to query and analyze your data afterwards, by using CQL (as
described within the next section).

How does it work? If you take a look into cassandra_ros/lib ... you will see a
couple of files in the form of CassandraTopic_'format'.py, which are all
descendants of CassandraTopic_.py. And each of them overwrites the methods:

`getColumnValidationClasses`: as the name suggests, is used to define the
column validation classes of the column-family * encode is used to encode the
ROS message into a Cassandra storable format * and decode translates it back,
from Cassandra to ROS.

As you could see so far, you only work with the single class of CassandraTopic.
In fact, this class inherits those methods, required for encoding and decoding
of messages, and only those of the required parent class. If you are seeking for
more information, have a look at the follwing link:
http://www.aizac.info/a-solution-to-the-diamond-problem-in-python/

So you are free to define an own format for conversion.

## CQL

If you want to analyze your data by using CQL, the CassandraQueryLanguage, you
will have to generate at first secondary indexes. Because this might be
expensive and not required for every part of a message, this has to be done
manually, as follows:

``` python
# this will automatically generate the appropriate index
rosCas.createIndex(topic, '.data')
```
For an automatic translation of topics, use the following method, this will
replace topicnames with their hash-value.

``` python
# will return everything
rosCas.exequteCQL('SELECT * FROM "test_topic"')

# only a list that contains all keys
rosCas.exequteCQL('SELECT KEY FROM "test_topic"')

# a list with all elements stored in .data
rosCas.exequteCQL('SELECT ".data" FROM "test_topic"')

# will return a list of keys
rosCas.exequteCQL('SELECT KEY FROM "test_topic" WHERE ".data"=\'test 12\';')

# if you want to get the whole messages bag, this requires 2 steps
# first of all store the keys of your statements, let us take the last
keys=rosCas.exequteCQL('SELECT KEY FROM "test_topic" WHERE ".data"=\'test 12\';')
messages = topicContainer.getData(keys)

# remove your indexes as follows
rosCas.removeIndex(topic, '.data')
```

For more information on CQL, check the following website:
http://www.datastax.com/docs/1.0/references/cql/index

# CassandraBag

If you already checked, if your package is working, than you already had used
cassandraBag. This is a simple service, which stores and replays messages
similar to rosbag. Simply start the application with:

``` bash
$ roslaunch cassandra_ros cassandraBag.launch
```
You can also change some of the parameters in the launch-file. Storing and
replaying or deleting topics requires to use the following command-line tool:

``` bash
$ rosrun cassandra_ros cassandraBag-cli.py
```
To get a list of all currently stored topics, use the parameter 'list':

``` bash
$ rosrun  cassandra_ros cassandraBag-cli.py list
```
Use record to store online messages, while option -f denotes the storage-format
rosrun cassandra_ros cassandraBag-cli.py record -f string start topic and use
record stop to finish recoring

``` bash
$ rosrun cassandra_ros cassandraBag-cli.py record stop topic
```
You can bag as many topics as Cassandra supports in parallel and stop and run
recording dynamically

``` bash
$ rosrun cassandra_ros cassandraBag-cli.py play start /usb_cam/image_raw/compressed
```
and if you want to delete some topics use delete, for more help, just run

``` bash
$ rosrun cassandra_ros cassandraBag-cli.py -h
```
or something like, which will support you with some command-line options ...

``` bash
$ rosrun cassandra_ros cassandraBag-cli.py play
```
