package org.elasticsearch.hadoop.handler;

import java.util.Properties;

/**
 * TODO: FILL THIS OUT
 */
public interface ErrorHandler<I, O> {

    public void init(Properties properties);

    public HandlerResult onError(I entry, ErrorCollector<O> collector) throws Exception;

    public void close();

}
