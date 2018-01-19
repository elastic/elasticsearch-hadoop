package org.elasticsearch.hadoop.rest.bulk.handler;

import java.util.concurrent.TimeUnit;

import org.elasticsearch.hadoop.handler.ErrorCollector;
import org.elasticsearch.hadoop.handler.HandlerResult;

/**
 * Any {@link ErrorCollector} instance that allows for a backoff period before executing a retry.
 */
public interface DelayableErrorCollector<T> extends ErrorCollector<T> {

    /**
     * Retry an operation (after the given backoff period) using the same data from the previous operation. Documents
     * retried through this method will be retried at the longest specified backoff time across all retries from its
     * bulk request.
     * @param timeAmount amount of time units to wait before retrying this document
     * @param timeUnits time units to measure backoff period in
     * @return Appropriate handler result value stating the failure has been retried. In
     * most cases this is {@link HandlerResult#HANDLED} but can vary depending on the use case.
     */
    public HandlerResult backoffAndRetry(long timeAmount, TimeUnit timeUnits);

    /**
     * Retry an operation (after the given backoff period) using the given retry data. Documents retried through
     * this method will be retried at the longest specified backoff time across all retries from its bulk request.
     * @param retryData operation/data to retry
     * @param timeAmount amount of time units to wait before retrying this document
     * @param timeUnits time units to measure backoff period in
     * @return Appropriate handler result value stating the failure has been retried. In
     * most cases this is {@link HandlerResult#HANDLED} but can vary depending on the use case.
     */
    public HandlerResult backoffAndRetry(T retryData, long timeAmount, TimeUnit timeUnits);
}
