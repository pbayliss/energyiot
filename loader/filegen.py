#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
import random
import datetime
from time import sleep
import csv
import sys

start_id = int(sys.argv[1])
stop_id = int(sys.argv[2])
data_dir = sys.argv[3]
def generate_sensor_data(start_id, stop_id):
    time = datetime.datetime.now()
    epochtime = int(time.strftime("%s"))
    start_time = epochtime - 86400
    results = []
    for device in range(start_id,stop_id+1):
        start_time = epochtime - 86400
        for j in range(1,97):
            results.append([str(device), datetime.datetime.fromtimestamp(start_time), 'KWH', random.uniform(0.1, 1.9)])
            start_time = start_time + 900
    return results

def main():
    results = generate_sensor_data(start_id,stop_id)
    b = open(data_dir+'sensordata'+str(datetime.datetime.now())+'.csv', 'wb')
    a = csv.writer(b)
    for result in results:
        a.writerow(result)
    b.close
    print str(stop_id-start_id+1)+' sensors data generated'

if __name__ == "__main__":
    main()
