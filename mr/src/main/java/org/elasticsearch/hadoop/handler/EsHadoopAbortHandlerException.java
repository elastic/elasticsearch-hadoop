package org.elasticsearch.hadoop.handler;

import org.elasticsearch.hadoop.EsHadoopException;

/**
 * Denotes that a handler has decided to abort the job on a potentially retryable value. Instead of displaying the
 * original error as the reason for aborting the operation, the message specified from this exception is used.
 */
public class EsHadoopAbortHandlerException extends EsHadoopException {

    public EsHadoopAbortHandlerException(String message, Throwable cause) {
        super(message, cause);
    }

    public EsHadoopAbortHandlerException(String message) {
        super(message);
    }
}
