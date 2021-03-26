package com.getindata.workshop.datagenerator;


import com.getindata.workshop.kafka.KafkaProperties;
import com.getindata.workshop.model.SongEventAvro;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaSerializationSchemaWrapper;

public class GenerationJobAvro {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        sEnv
                .addSource(new SongsSource())
                .addSink(
                        new FlinkKafkaProducer<>(
                                KafkaProperties.EVENTS_TOPIC,
                                new KafkaSerializationSchemaWrapper<>(
                                        KafkaProperties.EVENTS_TOPIC,
                                        null,
                                        false,
                                        ConfluentRegistryAvroSerializationSchema.forSpecific(
                                                SongEventAvro.class,
                                                SongEventAvro.class.getName(),
                                                KafkaProperties.SCHEMA_REGISTRY_URL)
                                ),
                                KafkaProperties.getKafkaProperties(),
                                FlinkKafkaProducer.Semantic.NONE
                        )
                );

        sEnv.execute("Songs generation job");
    }

}