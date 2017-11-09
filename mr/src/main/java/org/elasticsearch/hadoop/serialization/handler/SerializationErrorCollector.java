package org.elasticsearch.hadoop.serialization.handler;

import org.elasticsearch.hadoop.handler.ErrorCollector;
import org.elasticsearch.hadoop.handler.HandlerResult;

/**
 * TODO FILL IN
 */
public class SerializationErrorCollector<T> implements ErrorCollector<T> {

    private T retryValue = null;

    public T getAndReset() {
        T value = retryValue;
        retryValue = null;
        return value;
    }

    @Override
    public HandlerResult retry(T retryData) {
        retryValue = retryData;
        return HandlerResult.HANDLED;
    }
}
