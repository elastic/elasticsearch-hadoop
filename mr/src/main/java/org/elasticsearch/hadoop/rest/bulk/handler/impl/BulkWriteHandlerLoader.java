package org.elasticsearch.hadoop.rest.bulk.handler.impl;

import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.handler.HandlerLoader;
import org.elasticsearch.hadoop.rest.bulk.handler.BulkWriteErrorHandler;

/**
 * Produces BulkWriteErrorHandler objects based on user configuration.
 */
public class BulkWriteHandlerLoader extends HandlerLoader<BulkWriteErrorHandler> {

    public static final String ES_WRITE_REST_ERROR_HANDLERS = "es.write.rest.error.handlers";
    public static final String ES_WRITE_REST_ERROR_HANDLER = "es.write.rest.error.handler";

    public BulkWriteHandlerLoader() {
        super(BulkWriteErrorHandler.class);
    }

    @Override
    protected String getHandlersPropertyName() {
        return ES_WRITE_REST_ERROR_HANDLERS;
    }

    @Override
    protected String getHandlerPropertyName() {
        return ES_WRITE_REST_ERROR_HANDLER;
    }

    @Override
    protected BulkWriteErrorHandler loadBuiltInHandler(HandlerLoader.NamedHandlers handlerName) {
        switch (handlerName) {
            case FAIL:
                return new AbortOnFailure();
            case LOG:
                return new DropAndLog();
            default:
                throw new EsHadoopIllegalArgumentException(
                        "Could not find default implementation for built in handler type [" + handlerName + "]"
                );
        }
    }
}
