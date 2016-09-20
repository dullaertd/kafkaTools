#!/usr/bin/python
import sys, getopt
import datetime
import time

from kafka import KafkaConsumer
from kafka import TopicPartition

# Set output file
ftime = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
filename = "kafka-receive-stats-" + ftime
print ("writing output to: " + filename)
f1=open('./' + filename , 'w+')

# Set Defaults
topicName = "Lily_Data_Change_Capture"

# Get params
options, remainder = getopt.getopt( sys.argv[1:], 't:p:', ['topicname=','partition='])

for opt, arg in options:
    if opt in ('-t', '--topicname'):
        topicName = arg
    elif opt in ('-p', '--partition'):
        partitionNamed = int(arg)

print ("Getting messages for topic: " + topicName)

### Kafka Specific vars
brokers = ['kfkrplA1.uat.corp.telenet.be:9092','kfkrplB1.uat.corp.telenet.be:9092','kfkrplC1.uat.corp.telenet.be:9092']
consumer = KafkaConsumer(api_version='0.8.2', auto_offset_reset='earliest', consumer_timeout_ms=200000, bootstrap_servers=brokers)

print ("Connecting to brokers: " + str(brokers))

# Topic assignment
topics = []
for partition in consumer.partitions_for_topic(topicName):
    topics.append(TopicPartition(topicName, partition))

#topics = [TopicPartition('Lily_Data_Change_Capture', 2)]

consumer.assign(topics)
#consumer.seek_to_beginning()
consumer.seek_to_end()

nmbrOffsets = 0

for topic in topics:
     consumer.seek_to_end()
     print(str(topic) + " highest position: " + str(consumer.position(topic)))
     nmbrOffsets = nmbrOffsets + consumer.position(topic)
     #consumer.seek(topic,consumer.position(topic)-1)
     consumer.seek_to_beginning()
     print("new position: " + str(consumer.position(topic)))

print("Total amount of offsets: " + str(nmbrOffsets))

# Read all messages
startOfLoad = datetime.datetime.now()
nmbrMsg = 100000
currTime = datetime.datetime.now()
msg_length = 0

print (str(currTime) + " Starting fetch")

for idx, message in enumerate(consumer):
    msg_length = idx
    #print(str(message.key))
    #print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition, message.offset, message.key, message.value))
    #print(message.key)
    #print(message.value)

    if str(message.key) == "884215725|+flagBillShock":
        print("Key matching - " + str(message.key))
        print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition, message.offset, message.key, message.value))

    if idx % nmbrMsg == 0:
        #calc exec time
        prevTime = currTime
        currTime = datetime.datetime.now()
        elapsedTime = currTime - prevTime
        msgPerSec = round(nmbrMsg / elapsedTime.total_seconds())

        #clear list

        #print info
        print("[" + time.strftime("%Y-%m-%d %H:%M:%S") + "] " + str(idx) + " messages received (" + str(msgPerSec) + " msg/s)")
        f1.write("[" + time.strftime("%Y-%m-%d %H:%M:%S") + "] " + str(idx) + " messages received (" + str(msgPerSec) + " msg/s)")


endOfLoad = datetime.datetime.now()
totalDuration = endOfLoad - startOfLoad

consumer.close()

print("")
print("Received " + str(msg_length) + " messages in " + str(totalDuration))

f1.write("Received " + str(msg_length) + " messages in " + str(totalDuration))
f1.close()

