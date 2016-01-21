package org.vortex.domain;

public enum Status {
    IN_PROGRESS(1), FAILURE(2), SUCCESS(2), NONE(0);
    private int rank;

    Status(int rank) {
        this.rank = rank;
    }

    public boolean isSuccessful() {
        return SUCCESS == this;
    }

    public int rank() {
        return rank;
    }
}