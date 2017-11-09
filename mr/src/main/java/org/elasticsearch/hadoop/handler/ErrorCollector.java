package org.elasticsearch.hadoop.handler;

/**
 * Todo: Fill Out
 */
public interface ErrorCollector<T> {

    public HandlerResult retry(T retryData);

}
