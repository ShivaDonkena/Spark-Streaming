package com.core.bdcc.serde;

import com.core.bdcc.htm.MonitoringRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;

import java.util.Map;
import java.io.IOException;

public class KafkaJsonMonitoringRecordSerDe implements Deserializer<MonitoringRecord>, Serializer<MonitoringRecord> {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaJsonMonitoringRecordSerDe.class);

    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, MonitoringRecord data) {
        byte[] serializedData = null;
        try {
            serializedData = objectMapper.writeValueAsString(data).getBytes();
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            LOGGER.error(e.getMessage(), e);
        }
        return serializedData;
    }

    @Override
    public MonitoringRecord deserialize(String topic, byte[] data) {
        MonitoringRecord record = null;
        try {
            record = objectMapper.readValue(data, MonitoringRecord.class);
        } catch (IOException e) {
            e.printStackTrace();
            LOGGER.error(e.getMessage(), e);
        }
        return record;
    }

    @Override
    public void close() {
    }
}