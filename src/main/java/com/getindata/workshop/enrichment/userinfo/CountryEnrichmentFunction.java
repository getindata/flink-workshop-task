package com.getindata.workshop.enrichment.userinfo;

import com.getindata.workshop.enrichment.EnrichedSongEvent;
import com.getindata.workshop.model.SongEventAvro;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class CountryEnrichmentFunction
        extends KeyedCoProcessFunction<Integer, SongEventAvro, UserCdcEvent, EnrichedSongEvent> {

    public static final OutputTag<SongEventAvro> UNMATCHED_EVENTS =
            new OutputTag<>("unmatched-events", TypeInformation.of(SongEventAvro.class));

    private transient ValueState<String> countryState;

    @Override
    public void open(Configuration parameters) {
        this.countryState = getRuntimeContext().getState(new ValueStateDescriptor<>("user-country", String.class));
    }

    @Override
    public void processElement1(SongEventAvro event,
                                Context context,
                                Collector<EnrichedSongEvent> collector) throws Exception {
        String country = this.countryState.value();
        if (country != null) {
            collector.collect(
                    new EnrichedSongEvent(
                            event.getTimestamp(),
                            event.getUserId(),
                            event.getType(),
                            event.getSongId(),
                            null,
                            null,
                            country
                    )
            );
        } else {
            context.output(UNMATCHED_EVENTS, event);
        }
    }

    @Override
    public void processElement2(UserCdcEvent userCdcEvent,
                                Context context,
                                Collector<EnrichedSongEvent> collector) throws Exception {
        if (userCdcEvent.getOperation() == UserCdcEvent.Operation.DELETE) {
            this.countryState.clear();
        } else {
            this.countryState.update(userCdcEvent.getCountry());
        }
    }
}
