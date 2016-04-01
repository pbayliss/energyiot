#!/bin/bash
#start zookeeper
~/confluent*/bin/zookeeper-server-start ~/confluent*/etc/kafka/zookeeper.properties &
sleep 10
#start kafka
~/confluent*/bin/kafka-server-start ~/confluent*/etc/kafka/server.properties &
sleep 10
#start schema registry
~/confluent*/bin/schema-registry-start ~/confluent*//etc/schema-registry/schema-registry.properties &
sleep 10
#start kafa rest proxy
~/confluent*/bin/kafka-rest-start &
sleep 10
