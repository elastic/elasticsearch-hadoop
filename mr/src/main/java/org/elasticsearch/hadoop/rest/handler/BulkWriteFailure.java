package org.elasticsearch.hadoop.rest.handler;

import org.elasticsearch.hadoop.handler.Exceptional;

/**
 * Encapsulates all available information pertaining to an unhandled bulk indexing operation failure.
 */
public class BulkWriteFailure implements Exceptional {

    private final int response;
    private final Exception reason;

    public BulkWriteFailure(int response, Exception reason) {
        this.response = response;
        this.reason = reason;
    }

    /**
     * @return HTTP Response code for entry
     */
    public int getResponseCode() {
        return response;
    }

    @Override
    public Exception getException() {
        return reason;
    }

    /**
     * @return serialized bulk entry in byte array format
     */
    public byte[] getEntryContents() {
        //TODO: PULL THIS FROM THE TRACKING BYTES ARRAY
        return null;
    }
}
