package org.elasticsearch.hadoop.rest.handler;

import java.util.Properties;

/**
 * TODO: FILL THIS OUT
 */
public interface WriteErrorHandler {

    public void init(Properties properties);

    public HandlerResult onError(BulkWriteFailure entry, ErrorCollector collector);

    public void close();

}
