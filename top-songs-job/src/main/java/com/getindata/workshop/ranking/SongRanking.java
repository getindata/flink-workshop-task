package com.getindata.workshop.ranking;

import java.util.List;
import java.util.Objects;

public class SongRanking {

    private long windowStart;
    private long windowEnd;
    private List<SongRankingItem> topSongs;

    public SongRanking(long windowStart, long windowEnd, List<SongRankingItem> topSongs) {
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.topSongs = topSongs;
    }

    public long getWindowStart() {
        return windowStart;
    }

    public long getWindowEnd() {
        return windowEnd;
    }

    public List<SongRankingItem> getTopSongs() {
        return topSongs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SongRanking that = (SongRanking) o;
        return windowStart == that.windowStart &&
                windowEnd == that.windowEnd &&
                Objects.equals(topSongs, that.topSongs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(windowStart, windowEnd, topSongs);
    }

    @Override
    public String toString() {
        return "SongRanking{" +
                "windowStart=" + windowStart +
                ", windowEnd=" + windowEnd +
                ", topSongs=" + topSongs +
                '}';
    }
}
