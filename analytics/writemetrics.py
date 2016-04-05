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

if (len(sys.argv) < 3):
    sys.exit('Usage: %s <topic> <zookeeper_ip>' % sys.argv[0])

topic = sys.argv[1]
zk_ip = sys.argv[2]

schema_registry_client = CachedSchemaRegistryClient(url='http://'+ zk_ip + ':8081')
serializer = MessageSerializer(schema_registry_client)

def decoder(s):
    decoded_message = serializer.decode_message(s)
    return decoded_message

def getSqlContextInstance(sparkContext):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    return globals()['sqlContextSingletonInstance']

def process(time, rdd):
    print("========= %s =========" % str(time))
    try:
        # Get the singleton instance of SQLContext
        sqlContext = getSqlContextInstance(rdd.context)
        readingsDataFrame = sqlContext.createDataFrame(rdd).cache()
        readingsDataFrame.show(10)
        readingsDataFrame.write.format("org.apache.spark.sql.cassandra").\
            mode('append').options(table="raw_metrics", keyspace="metrics").save()
    except:
        pass

def main():
    #main function to execute code
    sqlContext = SQLContext(sc)
    zk_host = zk_ip+":2181"
    consumer_group = "reading-consumer-group"
    kafka_partitions={topic:1}
    #create kafka stream
    kvs = KafkaUtils.createStream(ssc,zk_host,consumer_group,kafka_partitions,valueDecoder=decoder)
    lines = kvs.map(lambda x: x[1])
    readings = lines.map(lambda x: Row(device_id=x["device_id"],\
        metric_time=datetime.datetime.fromtimestamp(int(x["metric_time"])),\
        metric_name=x["metric_name"],\
        metric_value=float(x["metric_value"])))
    readings.foreachRDD(process)
    ssc.start()
    ssc.awaitTermination()

if __name__ == "__main__":
    sc = SparkContext(appName="ReadingWriter")
    ssc = StreamingContext(sc,10)
    try:
        main()
    except KeyboardInterrupt:
        print("Gracefully stopping Spark Streaming Application")
        ssc.stop(True, True)
