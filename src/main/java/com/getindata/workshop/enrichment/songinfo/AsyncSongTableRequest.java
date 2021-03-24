package com.getindata.workshop.enrichment.songinfo;

import com.getindata.workshop.ranking.SongRanking;
import com.getindata.workshop.ranking.SongRankingItem;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static java.util.stream.Collectors.toList;

public class AsyncSongTableRequest extends RichAsyncFunction<SongRanking, SongRanking> {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncSongTableRequest.class);

    private transient Connection conn;

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName("com.mysql.jdbc.Driver");
        this.conn = DriverManager.getConnection("jdbc:mysql://mysql:3306/music_streaming", "mysqluser", "mysqlpw");
    }

    @Override
    public void close() throws Exception {
        this.conn.close();
    }

    @Override
    public void asyncInvoke(SongRanking ranking,
                            ResultFuture<SongRanking> resultFuture) {
        CompletableFuture
                .supplyAsync(new SongEnrichmentSupplier(conn, ranking))
                .thenAccept(result -> resultFuture.complete(Collections.singleton(result)));

    }

    private static class SongEnrichmentSupplier implements Supplier<SongRanking> {

        private final Connection conn;
        private final SongRanking ranking;

        SongEnrichmentSupplier(Connection conn, SongRanking ranking) {
            this.conn = conn;
            this.ranking = ranking;
        }

        @Override
        public SongRanking get() {
            return new SongRanking(
                    ranking.getWindowStart(),
                    ranking.getWindowEnd(),
                    ranking.getTopSongs().stream().map(this::getEnrichedSongEvent).collect(toList())
            );
        }

        private SongRankingItem getEnrichedSongEvent(SongRankingItem item) {
            try (PreparedStatement statement = conn.prepareStatement("select title, author from songs where id = ?")) {
                statement.setLong(1, item.getSongId());
                ResultSet rs = statement.executeQuery();
                if (rs.next()) {
                    LOG.warn("Song ID matched; ID={}.", item.getSongId());
                    String title = rs.getString(1);
                    String author = rs.getString(2);
                    return new SongRankingItem(
                            item.getSongId(),
                            title,
                            author,
                            item.getCount()
                    );
                } else {
                    LOG.warn("Song ID not matched; ID={}.", item.getSongId());
                    return null;
                }
            } catch (SQLException throwables) {
                LOG.error("Failed to fetch song details.", throwables);
                return null;
            }
        }
    }
}
