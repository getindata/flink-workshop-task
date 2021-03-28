package com.getindata.workshop.enrichment.userinfo;

import java.util.Objects;

import static java.lang.String.format;

public class UserCdcEvent {

    public enum Operation {
        SNAPSHOT, INSERT, UPDATE, DELETE;

        public static Operation getOperation(String operation) {
            switch (operation) {
                case "c":
                    return UserCdcEvent.Operation.INSERT;
                case "d":
                    return UserCdcEvent.Operation.DELETE;
                case "u":
                    return UserCdcEvent.Operation.UPDATE;
                case "r":
                    return UserCdcEvent.Operation.SNAPSHOT;
                default:
                    throw new IllegalArgumentException(format("Unknown operation %s.", operation));
            }
        }
    }

    private Operation operation;
    private long timestamp;
    private int id;
    private String firstName;
    private String lastName;
    private String country;

    public UserCdcEvent(Operation operation, long timestamp, int id, String firstName, String lastName, String country) {
        this.operation = operation;
        this.timestamp = timestamp;
        this.id = id;
        this.firstName = firstName;
        this.lastName = lastName;
        this.country = country;
    }

    public Operation getOperation() {
        return operation;
    }

    public void setOperation(Operation operation) {
        this.operation = operation;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UserCdcEvent that = (UserCdcEvent) o;
        return timestamp == that.timestamp &&
                id == that.id &&
                operation == that.operation &&
                Objects.equals(firstName, that.firstName) &&
                Objects.equals(lastName, that.lastName) &&
                Objects.equals(country, that.country);
    }

    @Override
    public int hashCode() {
        return Objects.hash(operation, timestamp, id, firstName, lastName, country);
    }

    @Override
    public String toString() {
        return "UserCdcEvent{" +
                "operation=" + operation +
                ", timestamp=" + timestamp +
                ", id=" + id +
                ", firstName='" + firstName + '\'' +
                ", lastName='" + lastName + '\'' +
                ", country='" + country + '\'' +
                '}';
    }
}
