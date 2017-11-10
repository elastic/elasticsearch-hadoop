package org.elasticsearch.hadoop.handler;

/**
 * An interface for accepting modified information to be retried in the event of a recoverable failure.
 */
public interface ErrorCollector<T> {

    /**
     * Retry an operation using the given retry data
     * @param retryData operation/data to retry
     * @return Appropriate handler result value stating the failure has been retried. In
     * most cases this is {@link HandlerResult#HANDLED} but can vary depending on the use case.
     */
    public HandlerResult retry(T retryData);

}
