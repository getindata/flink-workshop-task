package com.getindata.workshop.kafka;

import java.util.Properties;

public class KafkaProperties {

    public static final String EVENTS_TOPIC = "events";
    public static final String SCHEMA_REGISTRY_URL = "http://schema-registry:8082";

    public static Properties getKafkaProperties() {
        final Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "kafka:9092");
        return properties;
    }

}
