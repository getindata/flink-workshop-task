package com.getindata.workshop.enrichment.userinfo;

import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.getindata.workshop.enrichment.songinfo.AsyncSongTableRequest;
import com.getindata.workshop.enrichment.userinfo.UserCdcEvent.Operation;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.String.format;

public class UserDebeziumDeserializationSchema implements DebeziumDeserializationSchema<UserCdcEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncSongTableRequest.class);

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<UserCdcEvent> collector) {
        LOG.info("Received CDC event: {}", sourceRecord);
        Struct value = (Struct) sourceRecord.value();

        Struct source = value.getStruct("source");
        long timestamp = source.getInt64("ts_ms");

        Operation operation = Operation.getOperation(value.getString("op"));
        if (operation == Operation.DELETE) {
            Struct before = value.getStruct("before");
            Integer id = before.getInt32("id");
            collector.collect(new UserCdcEvent(operation, timestamp, id, null, null, null));
        } else {
            Struct after = value.getStruct("after");
            Integer id = after.getInt32("id");
            String firstName = after.getString("first_name");
            String lastName = after.getString("last_name");
            String country = after.getString("country");
            collector.collect(new UserCdcEvent(operation, timestamp, id, firstName, lastName, country));
        }
    }

    @Override
    public TypeInformation<UserCdcEvent> getProducedType() {
        return TypeInformation.of(UserCdcEvent.class);
    }
}
