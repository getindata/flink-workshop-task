/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.getindata.workshop;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.getindata.workshop.enrichment.EnrichedSongEvent;
import com.getindata.workshop.enrichment.songinfo.AsyncSongTableRequest;
import com.getindata.workshop.enrichment.userinfo.CountryEnrichmentFunction;
import com.getindata.workshop.enrichment.userinfo.UserCdcEvent;
import com.getindata.workshop.enrichment.userinfo.UserDebeziumDeserializationSchema;
import com.getindata.workshop.kafka.KafkaProperties;
import com.getindata.workshop.model.SongEventAvro;
import com.getindata.workshop.ranking.SongRanking;
import com.getindata.workshop.ranking.SongRankingItem;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.util.Collector;
import org.apache.flink.util.IterableUtils;

import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static java.util.stream.Collectors.toList;

public class StreamingJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final DataStream<SongEventAvro> events = env.addSource(getKafkaEventsSource());
        final DataStream<UserCdcEvent> usersMetadata = env.addSource(getUserMetadataSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<UserCdcEvent>forBoundedOutOfOrderness(Duration.ofMinutes(1L))
                                .withTimestampAssigner(new TimestampAssignerSupplier<UserCdcEvent>() {
                                    @Override
                                    public TimestampAssigner<UserCdcEvent> createTimestampAssigner(Context context) {
                                        return (userCdcEvent, l) -> Long.MAX_VALUE - 1;
                                    }
                                })
                );

        KeyedStream<SongEventAvro, Integer> keyedEvents = events
                .keyBy((KeySelector<SongEventAvro, Integer>) SongEventAvro::getUserId);

        KeyedStream<UserCdcEvent, Integer> keyedUserMetadata = usersMetadata
                .keyBy((KeySelector<UserCdcEvent, Integer>) UserCdcEvent::getId);

        SingleOutputStreamOperator<EnrichedSongEvent> enrichedEvents = keyedEvents
                .connect(keyedUserMetadata)
                .process(new CountryEnrichmentFunction());

        DataStream<SongEventAvro> unmatchedEvents = enrichedEvents
                .getSideOutput(CountryEnrichmentFunction.UNMATCHED_SONGS);

        SingleOutputStreamOperator<SongRanking> rawTopSongs = enrichedEvents
                .filter((FilterFunction<EnrichedSongEvent>) enrichedSongEvent -> enrichedSongEvent.getCountry().equals("Poland"))
                .keyBy((KeySelector<EnrichedSongEvent, Long>) EnrichedSongEvent::getSongId)
                .window(TumblingEventTimeWindows.of(Time.minutes(1L)))
                .aggregate(new SongAggregationFunction(), new SongWindowFunction())
                .windowAll(TumblingEventTimeWindows.of(Time.minutes(1L)))
                .process(new SelectTopSongs());

        SingleOutputStreamOperator<SongRanking> enrichedRanking =
                AsyncDataStream.unorderedWait(rawTopSongs, new AsyncSongTableRequest(), 5_000L, TimeUnit.MILLISECONDS);

        enrichedRanking
                .filter(Objects::nonNull)
                .print();

        env.execute("Top Songs ranking");
    }

    private static FlinkKafkaConsumerBase<SongEventAvro> getKafkaEventsSource() {
        return new FlinkKafkaConsumer<>(
                KafkaProperties.SONGS_TOPIC,
                ConfluentRegistryAvroDeserializationSchema.forSpecific(
                        SongEventAvro.class,
                        KafkaProperties.SCHEMA_REGISTRY_URL),
                KafkaProperties.getKafkaProperties()
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<SongEventAvro>forBoundedOutOfOrderness(Duration.ofMinutes(3L))
                        .withTimestampAssigner(new TimestampAssignerSupplier<SongEventAvro>() {
                            @Override
                            public TimestampAssigner<SongEventAvro> createTimestampAssigner(Context context) {
                                return (songEventAvro, l) -> songEventAvro.getTimestamp();
                            }
                        })
        );
    }

    private static DebeziumSourceFunction<UserCdcEvent> getUserMetadataSource() {
        return MySQLSource.<UserCdcEvent>builder()
                .hostname("mysql")
                .port(3306)
                .databaseList("music_streaming")
                .tableList("music_streaming.users")
                .username("debezium")
                .password("dbz")
                .deserializer(new UserDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();
    }

    private static class SongAggregationFunction implements AggregateFunction<EnrichedSongEvent, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(EnrichedSongEvent enrichedSongEvent, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long accumulator1, Long accumulator2) {
            return accumulator1 + accumulator2;
        }
    }

    private static class SongWindowFunction implements WindowFunction<Long, SongRankingItem, Long, TimeWindow> {
        @Override
        public void apply(Long key, TimeWindow timeWindow, Iterable<Long> iterable, Collector<SongRankingItem> collector) {
            long sum = 0;
            for (Long l : iterable) {
                sum += l;
            }
            collector.collect(new SongRankingItem(key, null, null, sum));
        }
    }

    private static class SelectTopSongs extends ProcessAllWindowFunction<SongRankingItem, SongRanking, TimeWindow> {

        @Override
        public void process(Context context, Iterable<SongRankingItem> iterable, Collector<SongRanking> collector) {
            List<SongRankingItem> topSongs = IterableUtils.toStream(iterable)
                    .sorted(Comparator.comparingLong(t -> -t.getCount()))
                    .limit(3)
                    .collect(toList());
            collector.collect(
                    new SongRanking(context.window().getStart(), context.window().getEnd(), topSongs)
            );
        }
    }

}
