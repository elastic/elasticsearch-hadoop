package org.elasticsearch.hadoop.rest.handler;

import org.elasticsearch.hadoop.handler.ErrorCollector;
import org.elasticsearch.hadoop.handler.HandlerResult;

/**
 * TODO: FILL OUT
 */
public class BulkWriteErrorCollector implements ErrorCollector<byte[]> {

    @Override
    public HandlerResult retry(byte[] retryData) {
        return HandlerResult.HANDLED;
    }

}
