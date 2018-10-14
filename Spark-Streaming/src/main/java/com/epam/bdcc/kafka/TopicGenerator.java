package com.core.bdcc.kafka;

import com.core.bdcc.htm.MonitoringRecord;
import com.core.bdcc.utils.GlobalConstants;
import com.core.bdcc.utils.PropertiesLoader;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class TopicGenerator implements GlobalConstants {
    private static final Logger LOGGER = LoggerFactory.getLogger(TopicGenerator.class);
    private static int batchSize, batchSleep;
    private static String topicName;

    public static void main(String[] args) {
        // load a properties file from class path, inside static method
        Properties applicationProperties = PropertiesLoader.getGlobalProperties();
        if (!applicationProperties.isEmpty()) {
            final boolean skipHeader = Boolean
                    .parseBoolean(applicationProperties.getProperty(GENERATOR_SKIP_HEADER_CONFIG));
            batchSize = Integer.parseInt(applicationProperties.getProperty(BATCH_SIZE_CONFIG));
            batchSleep = Integer.parseInt(applicationProperties.getProperty(GENERATOR_BATCH_SLEEP_CONFIG));
            final String sampleFile = applicationProperties.getProperty(GENERATOR_SAMPLE_FILE_CONFIG);
            topicName = applicationProperties.getProperty(KAFKA_RAW_TOPIC_CONFIG);
            KafkaProducer<String, MonitoringRecord> producer = KafkaHelper.createProducer();
            try {
                transferDataToKafka(readTheDeviceRecords(sampleFile),topicName,producer);
                producer.close();
            } catch (IOException e) {
                LOGGER.info(e.getMessage());
            }
        }
    }

    /*
     * Reads the sampleFile data using the bufferreader and then each line is added
     * to the list with type MonitoringRecord by creating the MonitoringRecord object passing each line as the splitted array.
     */
    public static List<MonitoringRecord> readTheDeviceRecords(String sampleFile) throws IOException {
        List<MonitoringRecord> result = new ArrayList<>();
        FileReader fileReader = new FileReader(sampleFile);
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            result.add(new MonitoringRecord(line.split(",")));
        }
        return result;
    }

    /*
    * This method will read the each monitoringRecord from the list containing the multiple monitoringRecords
    * and data will be send to the topic using the producer.
    */
    public static void transferDataToKafka(List<MonitoringRecord> lines,String topicName,Producer producer) {
        for (MonitoringRecord record : lines) {
            ProducerRecord<String, MonitoringRecord> producerRecord =
                    new ProducerRecord<>(topicName, KafkaHelper.getKey(record), record);
            producer.send(producerRecord);
            producer.flush();
			/*
			*Loging data example:
			*TopicName:[ monitoring20 ]Key:[ 10-001-0002-44201-1 ]Value:[ MonitoringRecord [stateCode=10, countyCode=001, siteNum=0002, parameterCode=44201, poc=1, latitude=38.986672,
			 *longitude=-75.5568, datum=WGS84, parameterName=Ozone, dateLocal=2014-07-29, timeLocal=18:00, dateGMT=2014-07-29, timeGMT=23:00, sampleMeasurement=0.039,
			 *unitsOfMeasure=Parts per million, mdl=0.005, uncertainty=, qualifier=, methodType=FEM, methodCode=047,
			 *methodName=INSTRUMENTAL - ULTRA VIOLET, stateName=Delaware, countyName=Kent, dateOfLastChange=2014-08-20, prediction=0.0, error=0.0, anomaly=0.0, predictionNext=0.0] ]
			*/
            LOGGER.info("TopicName:[ " + producerRecord.topic() + " ]" + "Key:[ " + producerRecord.key() + " ]" + "Value:[ " + producerRecord.value() + " ]");
        }
    }
}
