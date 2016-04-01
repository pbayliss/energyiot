#!/bin/bash

#create a topic based on the first argument of the bash script
~/confluent*/bin/kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic $1 &
sleep 10

#start the stream processing code
sudo ~/dse-*/bin/dse spark-submit --packages org.apache.spark:spark-streaming-kafka_2.10:1.4.1 analytics/writemetrics.py &
sleep 20

#simulate the sensor writes according to a number of sensors, a time interval, and a topic name
python simulator/generaterest.py $1 $2 $3 &
