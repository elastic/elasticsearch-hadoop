package org.elasticsearch.hadoop.rest.bulk.handler;

import java.util.concurrent.TimeUnit;

import org.elasticsearch.hadoop.EsHadoopIllegalStateException;
import org.elasticsearch.hadoop.handler.HandlerResult;

/**
 * An error collector that collects responses from code that handles document level bulk failures.
 */
public class BulkWriteErrorCollector implements DelayableErrorCollector<byte[]> {

    private long delayTimeInMillis;

    private boolean isRetry;
    private byte[] currentRetry;
    private String currentMessage;

    public BulkWriteErrorCollector() {
        this.delayTimeInMillis = 0L;
        this.isRetry = false;
        this.currentRetry = null;
        this.currentMessage = null;
    }

    @Override
    public HandlerResult retry() {
        isRetry = true;
        return HandlerResult.HANDLED;
    }

    @Override
    public HandlerResult backoffAndRetry(long timeAmount, TimeUnit timeUnits) {
        long givenDelayTimeInMillis = timeUnits.toMillis(timeAmount);
        if (givenDelayTimeInMillis > delayTimeInMillis) {
            delayTimeInMillis = givenDelayTimeInMillis;
        }
        return retry();
    }

    @Override
    public HandlerResult retry(byte[] retryData) {
        currentRetry = retryData;
        return retry();
    }

    @Override
    public HandlerResult backoffAndRetry(byte[] retryData, long timeAmount, TimeUnit timeUnits) {
        long givenDelayTimeInMillis = timeUnits.toMillis(timeAmount);
        if (givenDelayTimeInMillis > delayTimeInMillis) {
            delayTimeInMillis = givenDelayTimeInMillis;
        }
        return retry(retryData);
    }

    @Override
    public HandlerResult pass(String reason) {
        if (currentMessage == null) {
            currentMessage = reason;
        } else {
            throw new EsHadoopIllegalStateException("Error Handler is attempting to pass with a reason, but a " +
                    "reason already exists! Be sure to return the result of your call to errorCollector.pass(String), " +
                    "and call it only once per call to your Handler!");
        }
        return HandlerResult.PASS;
    }

    public boolean receivedRetries() {
        return isRetry;
    }

    public byte[] getAndClearRetryValue() {
        if (isRetry) {
            byte[] data = null;
            if (currentRetry != null) {
                data = currentRetry;
                currentRetry = null;
            }
            isRetry = false;
            return data;
        } else {
            return null;
        }
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
