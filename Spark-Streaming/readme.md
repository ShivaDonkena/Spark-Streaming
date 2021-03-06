Realtime Anomaly Detection
==========================

This project is a prototype for real time anomaly detection.
It uses [Numenta's Hierarchical Temporal Memory](https://numenta.org/) -  a technology which emulates the work of the cortex.

# What is done
1)	Everything concerning the intellectual part - the HTM network.

2)	Spark steaming application which takes the input stream from Kafka,
detects anomalies using online learning and outputs enriched records back into Kafka.

3) Prototype of visualization based on zeppelin notebook.

# What is not done yet
1) Optimise parameters and select best properties for real life run

2) Fix maven to not include the properties into the final jar. Add exclude spark libraries from uber jar.

# Run sequence

## One time actions
Start Zookeeper, Kafka and Zeppelin.

If you have Windows, [here](https://dzone.com/articles/running-apache-kafka-on-windows-os) 
is a nice blog about how to run Kafka on Windows.

### Zookeeper
Run Zookeeper:
```
bin\zkServer.cmd
```

### Kafka
Run Kafka:
```
bin\windows\kafka-server-start.bat .\config\server.properties
```

Create kafka topics called "monitoring20" and "monitoringEnriched2", e.g.:
```
bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 10 --topic monitoring20
bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic monitoringEnriched2
```

## Zeppelin
Run :
```
bin\zeppelin.cmd
```

Create spark2 interpreter from simple spark interpreter, by setting parameters like:
```
PYTHONPATH=%SPARK2_HOME%\python;%SPARK2_HOME%\python\lib\py4j-*-src.zip 
SPARK_HOME=%SPARK2_HOME%
SPARK_MAJOR_VERSION=2
spark.app.name=Zeppelin_Spark2
zeppelin.spark.enableSupportedVersionCheck=false 
```

Add following dependencies into spark2 interpreter:
```
org.apache.spark:spark-streaming-kafka-0-10_2.11:2.1.1
org.apache.kafka:kafka_2.11:0.10.2.1
org.apache.kafka:kafka-clients:0.10.2.1
org.apache.spark:spark-sql-kafka-0-10_2.11:2.1.1
```

Import anomaly-detector-kafka.json notebook into Zeppelin.

In case of problems with Windows path - replace spark-submit on spark-submit2
in bin\interpreter.cmd for spark.

## Running the application during development cycle

1) Start streaming application (com.core.bdcc.spark.AnomalyDetector), to wait for incoming data.

2) Start visualizer prototype in zeppelin:
    - Start "Visualizer Section".
    - Start "Streaming Section".
    - Use "Stop Streaming Application" to stop streaming section.

3) Run the com.core.bdcc.kafka.TopicGenerator, to fill in raw Kafka topic.
It takes records from csv (data\one_device_2015-2017.csv) and puts them into raw Kafka topic. 
See records schema in com.core.bdcc.htm.MonitoringRecord

![Graphs example](/notebook/img/graphs.png?raw=true "Running Example")

## Requirements
    Devices need to be persistently assigned to kafka topic partitions.
    Only one instance of HTMNetwork(deviceID) per devece (deviceID = com.core.bdcc.kafka.KafkaHelper.getKey(MonitoringRecord)) can be instantinated in spark streaming application.
    The order of the entries for the device must be saved.
    Max batch duration is 10 seconds! (Ideal is 2-3 seconds)
    All batches must be processed in the allotted interval, the first (HTMNetwork initialization) batch is an exception.
    KafkaProducer & ConsumerStrategy must be the following types (KafkaProducer<String, MonitoringRecord> & ConsumerStrategy<String, MonitoringRecord>).
    Restore from checkpoint is mandatory.
    
## Hints
    https://spark.apache.org/docs/latest/streaming-kafka-0-10-integration.html
    https://spark.apache.org/docs/latest/streaming-programming-guide.html#performance-tuning
    Code entry point:
        - com.core.bdcc.kafka.TopicGenerator (Kafka Record Generator).
        - com.core.bdcc.spark.AnomalyDetector (Spark Streaming Anomaly Detector).
    Kryo serializer is several times faster than JSON Serializer.
    There are a lot of options to optimise spark streaming & kafka, like:
        - spark.streaming.kafka.maxRatePerPartition
        - spark.streaming.backpressure.enabled
        - spark.serializer
        - checkpointInterval
        - GC options
        - ...
    
## Acceptance criteria:
    Missing parts are filled in and work meets all requirements
    Screenshot of SparkUI showing the various stages and tasks (especialy Streaming)
    Please include Spark logs as well (modify log4j.properties)
