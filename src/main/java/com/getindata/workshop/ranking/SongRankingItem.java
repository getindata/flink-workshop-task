package com.getindata.workshop.ranking;

import java.util.Objects;

public class SongRankingItem {

    private long songId;
    private String title;
    private String author;
    private long count;

    public SongRankingItem(long songId, String title, String author, long count) {
        this.songId = songId;
        this.title = title;
        this.author = author;
        this.count = count;
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

    public long getCount() {
        return count;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SongRankingItem that = (SongRankingItem) o;
        return songId == that.songId &&
                count == that.count &&
                Objects.equals(title, that.title) &&
                Objects.equals(author, that.author);
    }

    @Override
    public int hashCode() {
        return Objects.hash(songId, title, author, count);
    }

    @Override
    public String toString() {
        return "SongRankingItem{" +
                "songId=" + songId +
                ", title='" + title + '\'' +
                ", author='" + author + '\'' +
                ", count=" + count +
                '}';
    }
}
