package org.elasticsearch.hadoop.handler;

/**
 * Values that denote the result of an action taken by an {@link ErrorHandler}.
 */
public enum HandlerResult {

    /**
     * Signals to the connector that the given {@link ErrorHandler} was successfully able to handle the failure
     * scenario, either by swallowing the failure, persisting the operation for examination, or attempting to
     * retry the operation. In this case, the handling for this failure instance is stopped and no further handlers
     * in the chain will be called.
     */
    HANDLED,

    /**
     * Signals to the connector that the given {@link ErrorHandler} was unable to handle the failure scenario.
     * In this case, the failure information is passed on to the next configured handler in the chain.
     */
    PASS
}
