package org.elasticsearch.hadoop.serialization.handler;

import java.util.Properties;

import org.elasticsearch.hadoop.handler.ErrorHandler;
import org.elasticsearch.hadoop.handler.ErrorCollector;
import org.elasticsearch.hadoop.handler.Exceptional;
import org.elasticsearch.hadoop.handler.HandlerResult;

/**
 * TODO: FIll out
 */
public abstract class SerializationErrorHandler<T extends Exceptional> implements ErrorHandler<T, T> {

    public void init(Properties properties) { }

    public HandlerResult onError(T record, ErrorCollector<T> collector) throws Exception {
        return HandlerResult.PASS;
    }

    public void close() { }
}
