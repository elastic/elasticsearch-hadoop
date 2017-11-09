package org.elasticsearch.hadoop.rest.handler;

import java.util.Properties;

import org.elasticsearch.hadoop.handler.ErrorCollector;
import org.elasticsearch.hadoop.handler.ErrorHandler;
import org.elasticsearch.hadoop.handler.HandlerResult;

/**
 * TODO: FILL THIS OUT
 */
public abstract class BulkWriteErrorHandler implements ErrorHandler<BulkWriteFailure, byte[]> {

    public void init(Properties properties) {

    }

    public abstract HandlerResult onError(BulkWriteFailure entry, ErrorCollector<byte[]> collector) throws Exception;

    public void close() {

    }

}
