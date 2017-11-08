package org.elasticsearch.hadoop.rest.handler;

/**
 * TODO: FILL OUT
 */
public enum HandlerResult {
    // TODO: Make Javadocs
    PASS, // This connector cannot handle this error and the handling of the error should move on to the next handler
    HANDLED // This connector has handled this failure. No further handlers are needed to be called.
}
