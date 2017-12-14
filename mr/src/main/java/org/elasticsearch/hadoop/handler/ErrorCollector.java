package org.elasticsearch.hadoop.handler;

/**
 * An interface for accepting modified information to be retried in the event of a recoverable failure.
 */
public interface ErrorCollector<T> {

    /**
     * Retry an operation using the same data from the previous operation.
     * @return Appropriate handler result value stating the failure has been retried. In
     * most cases this is {@link HandlerResult#HANDLED} but can vary depending on the use case.
     */
    public HandlerResult retry();

    /**
     * Retry an operation using the given retry data
     * @param retryData operation/data to retry
     * @return Appropriate handler result value stating the failure has been retried. In
     * most cases this is {@link HandlerResult#HANDLED} but can vary depending on the use case.
     */
    public HandlerResult retry(T retryData);

    /**
     * Signal that a failure could not be handled by this handler and pass the record on to the next handler.
     * An optional string can be provided explaining the reason for passing on handling the error.
     * @param reason optional reason for passing on data. Reasons are used in the final report if an error could not be
     *               handled.
     * @return Appropriate handler result value stating the failure should be passed on to the next handler. In
     * most cases this is {@link HandlerResult#PASS} but can vary depending on the use case.
     */
    public HandlerResult pass(String reason);

}
