package org.elasticsearch.hadoop.rest.handler;

import org.elasticsearch.hadoop.handler.ErrorCollector;
import org.elasticsearch.hadoop.handler.HandlerResult;
import org.elasticsearch.hadoop.util.BytesArray;
import org.elasticsearch.hadoop.util.TrackingBytesArray;

/**
 * TODO: FILL OUT
 */
public class BulkWriteErrorCollector implements ErrorCollector<byte[]> {

    private boolean receivedRetries;
    private TrackingBytesArray retryBuffer;

    public BulkWriteErrorCollector(TrackingBytesArray retryBuffer) {
        this.receivedRetries = false;
        this.retryBuffer = retryBuffer;
    }

    @Override
    public HandlerResult retry(byte[] retryData) {
        receivedRetries = true;
        retryBuffer.copyFrom(new BytesArray(retryData));
        return HandlerResult.HANDLED;
    }

    public boolean receivedRetries() {
        return receivedRetries;
    }

    public TrackingBytesArray getBuffer() {
        return retryBuffer;
    }
}
