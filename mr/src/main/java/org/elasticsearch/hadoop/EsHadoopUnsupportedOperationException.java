package org.elasticsearch.hadoop;

/**
 * Denotes an operation that is not allowed to be performed, often due to the feature support of
 * the version of Elasticsearch being used.
 */
public class EsHadoopUnsupportedOperationException extends EsHadoopException {

    public EsHadoopUnsupportedOperationException() {
        super();
    }

    public EsHadoopUnsupportedOperationException(String message, Throwable cause) {
        super(message, cause);
    }

    public EsHadoopUnsupportedOperationException(String message) {
        super(message);
    }

    public EsHadoopUnsupportedOperationException(Throwable cause) {
        super(cause);
    }
}
