package org.elasticsearch.hadoop.rest.bulk.handler;

import java.io.InputStream;
import java.util.Collections;
import java.util.List;

import org.elasticsearch.hadoop.handler.Exceptional;
import org.elasticsearch.hadoop.util.BytesArray;
import org.elasticsearch.hadoop.util.FastByteArrayInputStream;

/**
 * Encapsulates all available information pertaining to an unhandled bulk indexing operation failure.
 */
public class BulkWriteFailure implements Exceptional {

    private final int response;
    private final Exception reason;
    private final BytesArray contents;
    private final int attemptNumber;
    private final List<String> passReasons;

    public BulkWriteFailure(int response, Exception reason, BytesArray contents, int attemptNumber, List<String> passReasons) {
        this.response = response;
        this.reason = reason;
        this.contents = contents;
        this.attemptNumber = attemptNumber;
        this.passReasons = Collections.unmodifiableList(passReasons);
    }

    /**
     * @return HTTP Response code for entry
     */
    public int getResponseCode() {
        return response;
    }

    // TODO: Use a better exception type?
    @Override
    public Exception getException() {
        return reason;
    }

    @Override
    public List<String> previousHandlerMessages() {
        return passReasons;
    }

    /**
     * @return serialized bulk entry in byte array format
     */
    public InputStream getEntryContents() {
        return new FastByteArrayInputStream(contents);
    }

    /**
     * The number of times that this version of the document has been sent to Elasticsearch. In the event that a document
     * is revised by a handler before being retried, this number will reset in the event of another failure. If a document
     * is retried without being revised, this number will increment across retries.
     *
     * @return the number of bulk write attempts.
     */
    public int getNumberOfAttempts() {
        return attemptNumber;
    }
}
