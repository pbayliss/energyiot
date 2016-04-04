##Prerequisites
Install the confluent package in your home directory ("~/"): 
>http://docs.confluent.io/2.0.1/installation.html#installation-archive  

Install dse in your home directory: 
>http://docs.datastax.com/en/datastax_enterprise/4.8/datastax_enterprise/install/installTARdse.html

Install pip: 
>https://pip.pypa.io/en/stable/installing/

Optional: 
Install, setup, and enter a virtualenv when running python code: 
>http://docs.python-guide.org/en/latest/dev/virtualenvs/  
Within the virtualenv install python dependencies:  
>`pip install requests`  
`pip install json`

Install confluent python library (virtualenv recommended): 
>https://github.com/verisign/python-confluent-schemaregistry

##USAGE
The following instructions assume you are in the `energyiot/` directory

###SETUP
Start dse:  
`scripts/setup/startdse.sh`

Create data model:  
`scripts/setup/createdatamodel.sh`  

Start Kafka components in a new window or screen:  
`scripts/setup/startkafka.sh`

Create Kafka topic:  
`scripts/setup/create_topic.sh <topic_name>`  
>For example:  
`scripts/setup/create_topic.sh meter_readings`

#Start Streaming job in a new window or screen, adding the topic name as a commandline argument
scripts/setup/start_streaming.sh meter_readings

#Run simulator in a new window or screen. Use the topic name you created in the create_topic script.
scripts/simulate_sensor_writes.sh <topic name> <number of sensors> <time interval>
#For example, the following inserts one reading into a topic named "meter_readings" for 100 sensors every 15 minutes:
scripts/simulate_sensor_writes.sh meter_readings 100 900

#Run the batch job
scripts/runbatch.sh

#You can verify the data is in the tables in cql:
> USE metrics;
> SELECT * FROM metrics LIMIT 100;
> SELECT * FROM metrics WHERE device_id='1';
