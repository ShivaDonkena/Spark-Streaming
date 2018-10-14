package com.core.bdcc.spark;

import com.core.bdcc.htm.HTMNetwork;
import com.core.bdcc.htm.MonitoringRecord;
import com.core.bdcc.htm.ResultState;
import com.core.bdcc.utils.GlobalConstants;
import com.core.bdcc.utils.PropertiesLoader;
import com.core.bdcc.kafka.KafkaHelper;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.StateSpec;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

import scala.Tuple2;

import java.util.HashMap;
import java.util.Properties;

public class AnomalyDetector implements GlobalConstants {
    private static Logger LOGGER = Logger.getLogger(AnomalyDetector.class);

    /**
     * TODO :
     * 1. Define Spark configuration (register serializers, if needed)
     * 2. Initialize streaming context with checkpoint directory
     * 3. Read records from kafka topic "monitoring20" (com.core.bdcc.kafka.KafkaHelper can help)
     * 4. Organized records by key and map with HTMNetwork state for each device
     * (com.core.bdcc.kafka.KafkaHelper.getKey - unique key for the device),
     * for detecting anomalies and updating HTMNetwork state (com.core.bdcc.spark.AnomalyDetector.mappingFunc can help)
     * 5. Send enriched records to topic "monitoringEnriched2" for further visualization
     **/
    public static void main(String[] args) throws Exception {
        //load a properties file from class path, inside static method
        final Properties applicationProperties = PropertiesLoader.getGlobalProperties();
        if (!applicationProperties.isEmpty()) {
            final String appName = applicationProperties.getProperty(SPARK_APP_NAME_CONFIG);
            final String rawTopicName = applicationProperties.getProperty(KAFKA_RAW_TOPIC_CONFIG);
            final String enrichedTopicName = applicationProperties.getProperty(KAFKA_ENRICHED_TOPIC_CONFIG);
            final String checkpointDir = applicationProperties.getProperty(SPARK_CHECKPOINT_DIR_CONFIG);
            final Duration batchDuration = Duration.apply(Long.parseLong(applicationProperties.getProperty(SPARK_BATCH_DURATION_CONFIG)));
            final Duration checkpointInterval = Duration.apply(Long.parseLong(applicationProperties.getProperty(SPARK_CHECKPOINT_INTERVAL_CONFIG)));
            // Spark configuration.
            SparkConf conf = new SparkConf()
                    .setMaster("local[2]")
                    .setAppName(appName);
            JavaStreamingContext jssc = new JavaStreamingContext(conf, batchDuration);
            //Checkpoint Directory
            jssc.checkpoint(checkpointDir);
            JavaInputDStream<ConsumerRecord<String, MonitoringRecord>> directStream = KafkaUtils.createDirectStream(
                    jssc,
                    LocationStrategies.PreferConsistent(), KafkaHelper.createConsumerStrategy(rawTopicName));
            //Checkpoint Interval
            directStream.checkpoint(checkpointInterval);
            //Creating the monitorMap -> JavaPairDStream for storing the records of topic monitoring20
            JavaPairDStream<String, MonitoringRecord> monitorMap = directStream
                    .mapToPair(record -> new Tuple2(KafkaHelper.getKey(record.value()), record.value()));

            monitorMap.mapWithState(StateSpec.function(mappingFunc))
                    .foreachRDD(rddData -> rddData.foreachPartition(item -> {
                        KafkaProducer<String, MonitoringRecord> producer = KafkaHelper.createProducer();
                        item.forEachRemaining(record -> {
                            ProducerRecord producerRecord = new ProducerRecord<>(enrichedTopicName, KafkaHelper.getKey(record), record);
                            //Sending the data to producer
                            producer.send(producerRecord); 
						  /**
						  	* Loging data example: 
     						* TopicName:[ monitoringEnriched2 ]Key:[ 10-001-0002-44201-1 ]Value:[ MonitoringRecord [stateCode=10, countyCode=001, siteNum=0002, parameterCode=44201, poc=1, latitude=38.986672,
							* longitude=-75.5568, datum=WGS84, parameterName=Ozone, dateLocal=2014-11-11, timeLocal=21:00, dateGMT=2014-11-12, timeGMT=02:00, sampleMeasurement=0.020, unitsOfMeasure=Parts per million
							*mdl=0.005, uncertainty=, qualifier=, methodType=FEM, methodCode=047, methodName=INSTRUMENTAL - ULTRA VIOLET, stateName=Delaware, countyName=Kent, dateOfLastChange=2014-12-30, prediction=0.022,
							*error=9.999999999999974E-4, anomaly=0.05, predictionNext=0.022] ]
     						*/
                            LOGGER.info("TopicName:[ " + producerRecord.topic() + " ]" + "Key:[ " + producerRecord.key() + " ]" + "Value:[ " + producerRecord.value() + " ]");
                        });
                        producer.close();
                    }));

            jssc.start();
            //This will wait till the process gets completed
            jssc.awaitTermination();

        }
    }

    /**
     * Computing using the  HTM algorithm used to find the anamologies in the data provided.
     */
    private static Function3<String, Optional<MonitoringRecord>, State<HTMNetwork>, MonitoringRecord> mappingFunc =
            (deviceID, recordOpt, state) ->
            {
                // case 0: timeout
                if (!recordOpt.isPresent())
                    return null;

                // either new or existing device
                if (!state.exists())
                    state.update(new HTMNetwork(deviceID));
                HTMNetwork htmNetwork = state.get();
                String stateDeviceID = htmNetwork.getId();
                if (!stateDeviceID.equals(deviceID))
                    throw new Exception("Wrong behaviour of Spark: stream key is $deviceID%s, while the actual state key is $stateDeviceID%s");
                MonitoringRecord record = recordOpt.get();
                // get the value of DT and Measurement and pass it to the HTM
                HashMap<String, Object> m = new java.util.HashMap<>();
                m.put("DT", DateTime.parse(record.getDateGMT() + " " + record.getTimeGMT(), DateTimeFormat.forPattern("YY-MM-dd HH:mm")));
                m.put("Measurement", Double.parseDouble(record.getSampleMeasurement()));
                ResultState rs = htmNetwork.compute(m);
                record.setPrediction(rs.getPrediction());
                record.setError(rs.getError());
                record.setAnomaly(rs.getAnomaly());
                record.setPredictionNext(rs.getPredictionNext());
                return record;
            };
}