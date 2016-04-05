#!/bin/bash

#run the streaming code
sudo ~/dse-*/bin/dse spark-submit --packages org.apache.spark:spark-streaming-kafka_2.10:1.4.1 analytics/writemetrics.py $1 $2
