package org.elasticsearch.hadoop.rest.handler;

import java.util.concurrent.TimeUnit;

import org.elasticsearch.hadoop.EsHadoopIllegalStateException;
import org.elasticsearch.hadoop.handler.HandlerResult;
import org.elasticsearch.hadoop.util.BytesArray;
import org.elasticsearch.hadoop.util.TrackingBytesArray;

/**
 * An error collector that collects responses from code that handles document level bulk failures.
 */
public class BulkWriteErrorCollector implements DelayableErrorCollector<byte[]> {

    private long delayTimeInMillis;
    private boolean receivedRetries;
    private TrackingBytesArray retryBuffer;

    private String currentMessage;

    public BulkWriteErrorCollector(TrackingBytesArray retryBuffer) {
        this.receivedRetries = false;
        this.retryBuffer = retryBuffer;
        this.delayTimeInMillis = 0L;
    }

    @Override
    public HandlerResult retry(byte[] retryData, long timeAmount, TimeUnit timeUnits) {
        long givenDelayTimeInMillis = timeUnits.toMillis(timeAmount);
        if (givenDelayTimeInMillis > delayTimeInMillis) {
            delayTimeInMillis = givenDelayTimeInMillis;
        }
        return retry(retryData);
    }

    @Override
    public HandlerResult retry(byte[] retryData) {
        receivedRetries = true;
        retryBuffer.copyFrom(new BytesArray(retryData));
        return HandlerResult.HANDLED;
    }

    @Override
    public HandlerResult pass(String reason) {
        if (currentMessage != null) {
            currentMessage = reason;
        } else {
            throw new EsHadoopIllegalStateException("Error Handler is attempting to pass with a reason, but a " +
                    "reason already exists! Be sure to return the result of your call to errorCollector.pass(String), " +
                    "and call it only once per call to your Handler!");
        }
        return HandlerResult.PASS;
    }

    public boolean receivedRetries() {
        return receivedRetries;
    }

    public TrackingBytesArray getBuffer() {
        return retryBuffer;
    }

    public String getAndClearMessage() {
        String msg = currentMessage;
        currentMessage = null;
        return msg;
    }

    public long getDelayTimeBetweenRetries() {
        return delayTimeInMillis;
    }
}
