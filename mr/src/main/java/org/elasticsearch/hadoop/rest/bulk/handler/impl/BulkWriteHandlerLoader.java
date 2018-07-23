package org.elasticsearch.hadoop.rest.bulk.handler.impl;

import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.handler.ErrorHandler;
import org.elasticsearch.hadoop.handler.impl.AbortOnFailure;
import org.elasticsearch.hadoop.handler.impl.AbstractHandlerLoader;
import org.elasticsearch.hadoop.handler.impl.DropAndLog;
import org.elasticsearch.hadoop.handler.impl.elasticsearch.ElasticsearchHandler;
import org.elasticsearch.hadoop.rest.bulk.handler.BulkWriteFailure;
import org.elasticsearch.hadoop.rest.bulk.handler.DelayableErrorCollector;
import org.elasticsearch.hadoop.rest.bulk.handler.IBulkWriteErrorHandler;

/**
 * Produces BulkWriteErrorHandler objects based on user configuration.
 */
public class BulkWriteHandlerLoader extends AbstractHandlerLoader<IBulkWriteErrorHandler> {

    public static final String ES_WRITE_REST_ERROR_HANDLERS = "es.write.rest.error.handlers";
    public static final String ES_WRITE_REST_ERROR_HANDLER = "es.write.rest.error.handler";

    public BulkWriteHandlerLoader() {
        super(IBulkWriteErrorHandler.class);
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
    protected IBulkWriteErrorHandler loadBuiltInHandler(AbstractHandlerLoader.NamedHandlers handlerName) {
        ErrorHandler<BulkWriteFailure, byte[], DelayableErrorCollector<byte[]>> genericHandler;
        switch (handlerName) {
            case FAIL:
                genericHandler = AbortOnFailure.create();
                break;
            case LOG:
                genericHandler = DropAndLog.create(new BulkLogRenderer());
                break;
            case ES:
                genericHandler = ElasticsearchHandler.create(getSettings(), new BulkErrorEventConverter());
                break;
            default:
                throw new EsHadoopIllegalArgumentException(
                        "Could not find default implementation for built in handler type [" + handlerName + "]"
                );
        }
        return new DelegatingErrorHandler(genericHandler);
    }
}
