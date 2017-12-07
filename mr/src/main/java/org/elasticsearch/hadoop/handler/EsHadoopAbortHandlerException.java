package org.elasticsearch.hadoop.handler;

import org.elasticsearch.hadoop.EsHadoopException;

/**
 * Denotes that a handler has decided to abort the job on a potentially retryable value. Instead of "inferring"
 * that a record should be aborted from an exception and including the original exception, this allows a handler
 * to return a message to the handling code that should be used instead of throwing multiple nested exceptions.
 */
public class EsHadoopAbortHandlerException extends EsHadoopException {

    public EsHadoopAbortHandlerException(String message) {
        super(message);
    }
}
