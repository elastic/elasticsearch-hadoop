package org.elasticsearch.hadoop.rest.handler;

import org.elasticsearch.hadoop.handler.Exceptional;
import org.elasticsearch.hadoop.util.BytesArray;

/**
 * Encapsulates all available information pertaining to an unhandled bulk indexing operation failure.
 */
public class BulkWriteFailure implements Exceptional {

    private final int response;
    private final Exception reason;
    private final BytesArray contents;

    public BulkWriteFailure(int response, Exception reason, BytesArray contents) {
        this.response = response;
        this.reason = reason;
        this.contents = contents;
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
        // TODO: Can we source this from tracking bytes array? Maybe as a fast byte array input stream?
        return contents.bytes();
    }
}
