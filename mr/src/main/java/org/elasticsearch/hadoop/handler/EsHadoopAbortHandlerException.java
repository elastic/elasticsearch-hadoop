package org.elasticsearch.hadoop.handler;

import org.elasticsearch.hadoop.EsHadoopException;

/**
 * Denotes that a handler has decided to abort the job on a potentially retryable value. Instead of displaying the
 * original error as the reason for aborting the operation, the message specified from this exception is used.
 *
 * Fixhere: Add exception cause - and log exception cause in bulk processor before treating as abort.
 */
public class EsHadoopAbortHandlerException extends EsHadoopException {

    public EsHadoopAbortHandlerException(String message) {
        super(message);
    }
}
