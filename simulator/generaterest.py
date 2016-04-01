import requests
import time
import random
import datetime
from time import sleep
import sys
import json

#initialize variables from command input
topic = sys.argv[1]
num_sensors = int(sys.argv[2])
interval = float(sys.argv[3])

url = 'http://localhost:8082/topics/'+topic
headers = {'Content-Type' : 'application/vnd.kafka.avro.v1+json'}

#Send dummy record with device_id "0" to retrieve value schema id.
def get_value_schema_id():
    reading_string = json.dumps({"value_schema": "{\"type\": \"record\", \"name\": \"Reading\", \"fields\": [{\"name\": \"device_id\", \"type\": \"string\"},{\"name\": \"metric_time\", \"type\": \"string\"},{\"name\": \"metric_name\", \"type\": \"string\"},{\"name\": \"metric_value\", \"type\": \"string\"}]}", "records": [{"value": {"device_id": "0","metric_time": datetime.datetime.now().strftime("%s"),"metric_name": "KwH","metric_value": str(random.uniform(0.1, 1.9))}}]})
    #reading_string = json.dumps(reading_string)
    return int(requests.post(url,data=reading_string,headers=headers).json()['value_schema_id'])

#simulate sensors
def simulate_rest_writes(num_sensors, interval):
    schema_id = get_value_schema_id()
    while(True):
        time = datetime.datetime.now().strftime("%s")
        for x in range (1,num_sensors+1):
            reading = [str(x),time,"KwH",str(random.uniform(0.1, 1.9))]
            #construct data json from reading, passing result of get_value_schema_id() as the value_schema_id.
            reading_payload = json.dumps({"value_schema_id": schema_id, "records": [{"value": {"device_id": reading[0],"metric_time": reading[1],"metric_name": reading[2],"metric_value": reading[3]}}]})
            response = requests.post(url,data=reading_payload,headers=headers)
            #responsejson = json.loads(response.text)
            #print response.json()['value_schema_id']
        sleep(interval)

def main():
    #simulate_rest_writes(num_sensors)
    simulate_rest_writes(num_sensors,interval)

if __name__ == "__main__":
    main()
