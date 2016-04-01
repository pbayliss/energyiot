#!/bin/bash

#create the topic
~/confluent*/bin/kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic $1
