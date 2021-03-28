package com.getindata.workshop.datagenerator;

import com.getindata.workshop.model.SongEventAvro;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;

class SongsSource extends RichParallelSourceFunction<SongEventAvro> {

    private static final long THREE_MINUTES_IN_MILLIS = 3 * 60 * 1000L;
    private static final long FIVE_MINUTES_IN_MILLIS = 5 * 60 * 1000L;

    private Random random;
    private boolean isRunning = true;

    @Override
    public void open(Configuration parameters) {
        this.random = new Random();
    }

    @Override
    public void run(SourceContext<SongEventAvro> sourceContext) throws Exception {
        while (isRunning) {
            sourceContext.collect(createEvent());
            Thread.sleep(500L);
        }
    }

    private SongEventAvro createEvent() {
        return SongEventAvro.newBuilder()
                .setSongId(random.nextInt(10) + 1)
                .setUserId(random.nextInt(10) + 1)
                .setTimestamp(System.currentTimeMillis() - getDelay())
                .setType("PLAY")
                .build();
    }

    private long getDelay() {
        if (random.nextInt(100) % 100 == 0) {
            // simulates late events
            return FIVE_MINUTES_IN_MILLIS;
        } else {
            // simulates out-of-orderness
            return Math.abs(random.nextLong() % (THREE_MINUTES_IN_MILLIS));
        }
    }

    @Override
    public void cancel() {
        this.isRunning = false;
    }
}
