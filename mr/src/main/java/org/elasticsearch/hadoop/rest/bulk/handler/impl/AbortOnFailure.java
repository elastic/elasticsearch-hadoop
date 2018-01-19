package org.elasticsearch.hadoop.rest.bulk.handler.impl;

import org.elasticsearch.hadoop.handler.HandlerResult;
import org.elasticsearch.hadoop.rest.bulk.handler.BulkWriteErrorHandler;
import org.elasticsearch.hadoop.rest.bulk.handler.BulkWriteFailure;
import org.elasticsearch.hadoop.rest.bulk.handler.DelayableErrorCollector;

/**
 * Aborts on failure.
 */
public class AbortOnFailure extends BulkWriteErrorHandler {

    @Override
    public HandlerResult onError(BulkWriteFailure entry, DelayableErrorCollector<byte[]> collector) throws Exception {
        return HandlerResult.ABORT;
    }

}
