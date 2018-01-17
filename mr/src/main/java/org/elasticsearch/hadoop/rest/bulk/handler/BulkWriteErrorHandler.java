package org.elasticsearch.hadoop.rest.bulk.handler;

import java.util.Properties;

import org.elasticsearch.hadoop.handler.ErrorHandler;
import org.elasticsearch.hadoop.handler.HandlerResult;

/**
 * {@link ErrorHandler} subclass that allows for the interception of failures related to bulk write failures.
 */
public abstract class BulkWriteErrorHandler implements ErrorHandler<BulkWriteFailure, byte[], DelayableErrorCollector<byte[]>> {
    public void init(Properties properties) {}

    public abstract HandlerResult onError(BulkWriteFailure entry, DelayableErrorCollector<byte[]> collector) throws Exception;

    public void close() {}
}
