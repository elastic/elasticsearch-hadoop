package org.elasticsearch.hadoop.handler;

import java.util.Properties;

/**
 * Handler interface to be notified of and handle recoverable errors during connector execution.
 */
public interface ErrorHandler<I extends Exceptional, O> {

    /**
     * Called at the handler creation time to initialize any internal state or resources.
     * @param properties Properties for this handler with handler name prefix stripped away.
     */
    public void init(Properties properties);

    /**
     * Called when an exception or failure occurs in a part of the connector.
     * @param entry information about the failure, normally includes operational data and error information
     * @param collector handler for accepting user reactions to the failure, like retrying with modified parameters
     * @return An enum that describes the handler's result, either that it handled the error or if the error should be
     * passed on to the next handler.
     * @throws Exception In the event that the current failure should not be handled, and should halt the connector
     * processing.
     */
    public HandlerResult onError(I entry, ErrorCollector<O> collector) throws Exception;

    /**
     * Called at the close of the connector to clean up any internal resources.
     */
    public void close();

    /**
     * The error handlers take advantage of a lot of generics. Thus it is difficult to verify that you have provided
     * type-safe implementations at runtime without checking that you have extended from one of the abstract classes.
     */
    public void __implement_the_abstract_class_instead__();

}
