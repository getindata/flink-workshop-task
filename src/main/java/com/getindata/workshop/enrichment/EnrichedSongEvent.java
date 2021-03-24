package com.getindata.workshop.enrichment;

import java.util.Objects;

public class EnrichedSongEvent {

    private long timestamp;
    private int userId;
    private String type;
    private long songId;
    private String title;
    private String author;
    private String country;

    public EnrichedSongEvent(long timestamp, int userId, String type, long songId, String title, String author, String country) {
        this.timestamp = timestamp;
        this.userId = userId;
        this.type = type;
        this.songId = songId;
        this.title = title;
        this.author = author;
        this.country = country;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public int getUserId() {
        return userId;
    }

    public String getType() {
        return type;
    }

    public long getSongId() {
        return songId;
    }

    public String getTitle() {
        return title;
    }

    public String getAuthor() {
        return author;
    }

    public String getCountry() {
        return country;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EnrichedSongEvent that = (EnrichedSongEvent) o;
        return timestamp == that.timestamp &&
                userId == that.userId &&
                songId == that.songId &&
                Objects.equals(type, that.type) &&
                Objects.equals(title, that.title) &&
                Objects.equals(author, that.author) &&
                Objects.equals(country, that.country);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, userId, type, songId, title, author, country);
    }

    @Override
    public String toString() {
        return "EnrichedSongEvent{" +
                "timestamp=" + timestamp +
                ", userId=" + userId +
                ", type='" + type + '\'' +
                ", songId=" + songId +
                ", title='" + title + '\'' +
                ", author='" + author + '\'' +
                ", country='" + country + '\'' +
                '}';
    }
}
