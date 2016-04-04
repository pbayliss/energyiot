#!/bin/bash

#generate file
python ./loader/filegen.py $1 $2 $3 $4

#load file
~/cassandra-loader -f $3 -schema "metrics.raw_metrics(device_id, metric_time, metric_name, metric_value)" -dateFormat "yyyy-MM-dd HH:mm:ss" -host $4
