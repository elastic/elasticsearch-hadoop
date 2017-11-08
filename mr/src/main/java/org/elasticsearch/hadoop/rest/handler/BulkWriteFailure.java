package org.elasticsearch.hadoop.rest.handler;

/**
 * Todo: Fill Out
 */
public class BulkWriteFailure {

    private final int response;
    private final Throwable reason;

    public BulkWriteFailure(int response, Throwable reason) {
        this.response = response;
        this.reason = reason;
    }

    public int getResponseCode() {
        return response;
    }

    public Throwable getReason() {
        return reason;
    }

    public byte[] getEntryContents() {
        //TODO: PULL THIS FROM THE TRACKING BYTES ARRAY
        return null;
    }


}
