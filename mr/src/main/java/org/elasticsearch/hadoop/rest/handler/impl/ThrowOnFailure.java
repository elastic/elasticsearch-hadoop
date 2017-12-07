package org.elasticsearch.hadoop.rest.handler.impl;

import org.elasticsearch.hadoop.handler.HandlerResult;
import org.elasticsearch.hadoop.rest.handler.BulkWriteErrorHandler;
import org.elasticsearch.hadoop.rest.handler.BulkWriteFailure;
import org.elasticsearch.hadoop.rest.handler.DelayableErrorCollector;

/**
 * Throws on failure.
 */
public class ThrowOnFailure extends BulkWriteErrorHandler {

    @Override
    public HandlerResult onError(BulkWriteFailure entry, DelayableErrorCollector<byte[]> collector) throws Exception {
        throw entry.getException();
    }

}
