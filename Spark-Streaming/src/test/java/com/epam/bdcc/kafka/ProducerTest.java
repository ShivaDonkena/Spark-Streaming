
package com.core.bdcc.kafka;

import com.core.bdcc.htm.MonitoringRecord;
import com.core.bdcc.serde.KafkaJsonMonitoringRecordSerDe;
import com.core.bdcc.utils.PropertiesLoader;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static com.core.bdcc.utils.GlobalConstants.KAFKA_RAW_TOPIC_CONFIG;
import static org.junit.Assert.assertEquals;

public class ProducerTest {
    MockProducer<String, MonitoringRecord> producer;
    ArrayList<MonitoringRecord> MonitoringRecordsTest;

    @Before
    public void setUp() {
        producer = new MockProducer<>(false,
                new StringSerializer(),
                new KafkaJsonMonitoringRecordSerDe());
        MonitoringRecordsTest = new ArrayList<>();
        MonitoringRecordsTest.add(new MonitoringRecord("10,001,0002,44201,1,38.986672,-75.5568,WGS84,Ozone,2014-01-01,00:00,2014-01-01,05:00,0.016,Parts per million,0.005,,,FEM,047,INSTRUMENTAL - ULTRA VIOLET,Delaware,Kent,2014-02-12".split(",")));
        MonitoringRecordsTest.add(new MonitoringRecord("10,001,0002,44201,1,38.986672,-75.5568,WGS84,Ozone,2014-01-01,01:00,2014-01-01,06:00,0.016,Parts per million,0.005,,,FEM,047,INSTRUMENTAL - ULTRA VIOLET,Delaware,Kent,2014-02-12".split(",")));
    }

    /**
     * To test the output of the producer setting the mock MonitoringRecords list as the input and then comparing that with the Producer record that will generated.
     */
    @Test
    public void producerTest() {
        Properties applicationProperties = PropertiesLoader.getGlobalProperties();
        final String topicName = applicationProperties.getProperty(KAFKA_RAW_TOPIC_CONFIG);
        TopicGenerator.transferDataToKafka(MonitoringRecordsTest, topicName, producer);
        List<ProducerRecord<String, MonitoringRecord>> history = producer.history();
        List<ProducerRecord<String, MonitoringRecord>> expected = Arrays.asList(
                new ProducerRecord<>(topicName, "10-001-0002-44201-1",
                        new MonitoringRecord("10,001,0002,44201,1,38.986672,-75.5568,WGS84,Ozone,2014-01-01,00:00,2014-01-01,05:00,0.016,Parts per million,0.005,,,FEM,047,INSTRUMENTAL - ULTRA VIOLET,Delaware,Kent,2014-02-12".split(","))
                ),
                new ProducerRecord<>(topicName, "10-001-0002-44201-1",
                        new MonitoringRecord("10,001,0002,44201,1,38.986672,-75.5568,WGS84,Ozone,2014-01-01,01:00,2014-01-01,06:00,0.016,Parts per million,0.005,,,FEM,047,INSTRUMENTAL - ULTRA VIOLET,Delaware,Kent,2014-02-12".split(","))
                ));
        assertEquals(expected, history);
    }

    /**
     * To test the list that is to be generated for MonitoringRecords from the csv file with the expected list.
     */
    @Test
    public void testListOfMonitoringRecords() {
        try {
            List<MonitoringRecord> records = TopicGenerator.readTheDeviceRecords("D:\\homework\\src\\test\\resources\\test.csv");
            assertEquals(MonitoringRecordsTest, records);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}

