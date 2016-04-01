import sys
import datetime
import json

from pyspark import SparkContext

from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from pyspark.sql import Row
from pyspark.sql.context import SQLContext

from confluent.schemaregistry.client import CachedSchemaRegistryClient
from confluent.schemaregistry.serializers import MessageSerializer



zk_ip = 'localhost'

schema_registry_client = CachedSchemaRegistryClient(url='http://'+ zk_ip + ':8081')
serializer = MessageSerializer(schema_registry_client)

def decoder(s):
    decoded_message = serializer.decode_message(s)
    return decoded_message

def main():
#main function to execute code
    sc = SparkContext(appName="ReadingWriter")
    ssc = StreamingContext(sc,10)
    sqlContext = SQLContext(sc)
    zk_host = zk_ip+":2181"
    consumer_group = "reading-consumer-group"
    kafka_partitions={"amtest":1}
    #create kafka stream
    kvs = KafkaUtils.createStream(ssc,zk_host,consumer_group,kafka_partitions,valueDecoder=decoder)
    lines = kvs.map(lambda x: x[1])
    #readings = lines.map(lambda x: {"device_id":x["device_id"],"metric_time":x["metric_time"],"metric_name":x["metric_name"],"metric_value":x["metric_value"]})
    readings = lines.map(lambda x: {"device_id":x["device_id"],"metric_time":datetime.datetime.fromtimestamp(int(x["metric_time"])),"metric_name":x["metric_name"],"metric_value":float(x["metric_value"])})
    readings.foreachRDD(lambda rdd: rdd.saveToCassandra("metrics", "raw_metrics"))
    #readingdf.show()
    #readings.pprint()
    #lines.saveToCassandra("metrics", "raw_metrics")
    ssc.start()
    ssc.awaitTermination()
if __name__ == "__main__":
    main()
