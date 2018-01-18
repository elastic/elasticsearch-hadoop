package org.elasticsearch.hadoop.rest.bulk;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.hadoop.EsHadoopException;
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.EsHadoopIllegalStateException;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.handler.EsHadoopAbortHandlerException;
import org.elasticsearch.hadoop.handler.HandlerResult;
import org.elasticsearch.hadoop.rest.ErrorExtractor;
import org.elasticsearch.hadoop.rest.Resource;
import org.elasticsearch.hadoop.rest.RestClient;
import org.elasticsearch.hadoop.rest.bulk.handler.BulkWriteErrorCollector;
import org.elasticsearch.hadoop.rest.bulk.handler.BulkWriteErrorHandler;
import org.elasticsearch.hadoop.rest.bulk.handler.BulkWriteFailure;
import org.elasticsearch.hadoop.rest.bulk.handler.impl.BulkWriteHandlerLoader;
import org.elasticsearch.hadoop.rest.bulk.handler.impl.HttpRetryHandler;
import org.elasticsearch.hadoop.rest.stats.Stats;
import org.elasticsearch.hadoop.rest.stats.StatsAware;
import org.elasticsearch.hadoop.util.ArrayUtils;
import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.BytesArray;
import org.elasticsearch.hadoop.util.BytesRef;
import org.elasticsearch.hadoop.util.TrackingBytesArray;
import org.elasticsearch.hadoop.util.unit.TimeValue;

/**
 * Oversees the addition of bulk entries into an internal buffer, the flushing of documents to Elasticsearch,
 * and the handling of failures in bulk operations.
 */
public class BulkProcessor implements Closeable, StatsAware {

    private static Log LOG = LogFactory.getLog(BulkProcessor.class);

    private final RestClient restClient;
    private final Resource resource;
    private final Settings settings;
    private final Stats stats = new Stats();
    private final ErrorExtractor errorExtractor;

    // Buffers and state of content
    private BytesArray ba;
    private TrackingBytesArray data;
    private int dataEntries = 0;

    // Configs
    private int bufferEntriesThreshold;
    private boolean autoFlush = true;
    private int retryLimit;

    // Processor writing state flags
    private boolean executedBulkWrite = false;
    private boolean hadWriteErrors = false;
    private boolean requiresRefreshAfterBulk = false;

    // Bulk write error handlers.
    private List<BulkWriteErrorHandler> documentBulkErrorHandlers;

    public BulkProcessor(RestClient restClient, Resource resource, Settings settings) {
        this.restClient = restClient;
        this.resource = resource;
        this.settings = settings;

        // Flushing bounds
        this.autoFlush = !settings.getBatchFlushManual();
        this.bufferEntriesThreshold = settings.getBatchSizeInEntries();
        this.requiresRefreshAfterBulk = settings.getBatchRefreshAfterWrite();
        this.retryLimit = settings.getBatchWriteRetryLimit();

        // Backing data array
        this.ba = new BytesArray(new byte[settings.getBatchSizeInBytes()], 0);
        this.data = new TrackingBytesArray(ba);

        // Create error handlers
        BulkWriteErrorHandler httpRetryHandler = new HttpRetryHandler(settings);
        BulkWriteHandlerLoader handlerLoader = new BulkWriteHandlerLoader();
        handlerLoader.setSettings(settings);

        // Order up the handlers.
        this.documentBulkErrorHandlers = new ArrayList<BulkWriteErrorHandler>();
        this.documentBulkErrorHandlers.add(httpRetryHandler);
        this.documentBulkErrorHandlers.addAll(handlerLoader.loadHandlers());

        // Error Extractor
        this.errorExtractor = new ErrorExtractor(settings.getInternalVersionOrThrow());

    }

    /**
     * Adds an entry to the bulk request, potentially flushing if the request reaches capacity.
     * @param payload the entire bulk entry in JSON format, including the header and payload.
     */
    public void add(BytesRef payload) {
        // check space first
        // ba is the backing array for data
        if (payload.length() > ba.available()) {
            if (autoFlush) {
                flush();
            }
            else {
                throw new EsHadoopIllegalStateException(
                        String.format("Auto-flush disabled and bulk buffer full; disable manual flush or increase " +
                                "capacity [current size %s]; bailing out", ba.capacity()));
            }
        }

        data.copyFrom(payload);

        dataEntries++;
        if (bufferEntriesThreshold > 0 && dataEntries >= bufferEntriesThreshold) {
            if (autoFlush) {
                flush();
            }
            else {
                // handle the corner case of manual flush that occurs only after the buffer is completely full (think size of 1)
                if (dataEntries > bufferEntriesThreshold) {
                    throw new EsHadoopIllegalStateException(
                            String.format(
                                    "Auto-flush disabled and maximum number of entries surpassed; disable manual " +
                                            "flush or increase capacity [current size %s]; bailing out",
                                    bufferEntriesThreshold));
                }
            }
        }
    }

    /**
     * Keeps track of a given document entry's position in the original bulk request, as well as how many
     * attempts to write the entry have been performed.
     */
    private class BulkAttempt {
        public BulkAttempt(int attemptNumber, int originalPosition) {
            this.attemptNumber = attemptNumber;
            this.originalPosition = originalPosition;
        }

        private int attemptNumber;
        private int originalPosition;
    }

    /**
     * Attempts a flush operation, handling failed documents based on configured error listeners.
     * @return A result object detailing the success or failure of the request, including information about any
     * failed documents.
     * @throws EsHadoopException in the event that the bulk operation fails or is aborted.
     */
    public BulkResponse tryFlush() {
        BulkResponse bulkResult = null;
        boolean trackingArrayExpanded = false;

        try {
            // double check data - it might be a false flush (called on clean-up)
            if (data.length() > 0) {
                int totalDocs = data.entries();
                int docsSent = 0;
                int docsSkipped = 0;
                int docsAborted = 0;
                boolean retryOperation = false;
                int totalAttempts = 0;
                long waitTime = 0L;
                List<BulkAttempt> retries = new ArrayList<BulkAttempt>();
                List<BulkResponse.BulkError> abortErrors = new ArrayList<BulkResponse.BulkError>();

                do {
                    if (totalAttempts > retryLimit) {
                        throw new EsHadoopException("Executed too many bulk requests without success. Attempted [" +
                                totalAttempts + "] write operations, which exceeds the bulk request retry limit specified" +
                                "by [" + ConfigurationOptions.ES_BATCH_WRITE_RETRY_LIMIT + "], and found data still " +
                                "not accepted. Perhaps there is an error handler that is not terminating? Bailing out..."
                        );
                    }

                    // Log messages, and if wait time is set, perform the thread sleep.
                    initFlushOperation(retryOperation, waitTime);

                    // Exec bulk operation to ES, get response.
                    RestClient.BulkActionResponse bar = restClient.bulk(resource, data);
                    totalAttempts++;

                    // Log retry stats if relevant
                    if (retryOperation) {
                        stats.docsRetried += data.entries();
                        stats.bytesRetried += data.length();
                        stats.bulkRetries++;
                        stats.bulkRetriesTotalTime += bar.getTimeSpent();
                    }
                    executedBulkWrite = true;

                    // Handle bulk write failures
                    if (!bar.getEntries().hasNext()) {
                        // Legacy Case:
                        // If no items on response, assume all documents made it in.
                        // Recorded bytes are ack'd here
                        stats.bytesAccepted += data.length();
                        stats.docsAccepted += data.entries();
                        retryOperation = false;
                        bulkResult = BulkResponse.complete(bar.getResponseCode(), bar.getTimeSpent(), totalDocs, totalDocs, 0);
                    } else {
                        // Base Case:
                        // Iterate over the response and the data in the tracking bytes array at the same time, passing
                        // errors to error handlers for resolution.

                        // Keep track of which document we are on as well as where we are in the tracking bytes array.
                        int documentNumber = 0;
                        int trackingBytesPosition = 0;

                        // Hand off the previous list of retries so that we can track the next set of retries (if any).
                        List<BulkAttempt> previousRetries = retries;
                        retries = new ArrayList<BulkAttempt>();

                        // If a document is edited and retried then it is added at the end of the buffer. Keep a tail list of these new retry attempts.
                        List<BulkAttempt> newDocumentRetries = new ArrayList<BulkAttempt>();

                        BulkWriteErrorCollector errorCollector = new BulkWriteErrorCollector();

                        // Iterate over all entries, and for each error found, attempt to handle the problem.
                        for (Iterator<Map> iterator = bar.getEntries(); iterator.hasNext(); ) {

                            // The array of maps are (operation -> document info) maps
                            Map map = iterator.next();
                            // Get the underlying document information as a map and extract the error information.
                            Map values = (Map) map.values().iterator().next();
                            Integer docStatus = (Integer) values.get("status");
                            String error = errorExtractor.extractError(values);

                            if (error == null || error.isEmpty()){
                                // Write operation for this entry succeeded
                                stats.bytesAccepted += data.length(trackingBytesPosition);
                                stats.docsAccepted += 1;
                                docsSent += 1;
                                data.remove(trackingBytesPosition);
                            } else {
                                // Found a failed write
                                BytesArray document = data.entry(trackingBytesPosition);

                                // In pre-2.x ES versions, the status is not included.
                                int status = docStatus == null ? -1 : docStatus;

                                // Figure out which attempt number sending this document was and which position the doc was in
                                BulkAttempt previousAttempt;
                                if (previousRetries.isEmpty()) {
                                    // No previous retries, create an attempt for the first run
                                    previousAttempt = new BulkAttempt(1, documentNumber);
                                } else {
                                    // Grab the previous attempt for the document we're processing, and bump the attempt number.
                                    previousAttempt = previousRetries.get(documentNumber);
                                    previousAttempt.attemptNumber++;
                                }

                                // Handle bulk write failures
                                // Todo: We should really do more with these bulk error pass reasons if the final outcome is an ABORT.
                                List<String> bulkErrorPassReasons = new ArrayList<String>();
                                BulkWriteFailure failure = new BulkWriteFailure(
                                        status,
                                        new Exception(error),
                                        document,
                                        previousAttempt.attemptNumber,
                                        bulkErrorPassReasons
                                );

                                // Label the loop since we'll be breaking to/from it within a switch block.
                                handlerLoop: for (BulkWriteErrorHandler errorHandler : documentBulkErrorHandlers) {
                                    HandlerResult result;
                                    try {
                                        result = errorHandler.onError(failure, errorCollector);
                                    } catch (EsHadoopAbortHandlerException ahe) {
                                        // Count this as an abort operation, but capture the error message from the
                                        // exception as the reason. Log any cause since it will be swallowed.
                                        Throwable cause = ahe.getCause();
                                        if (cause != null) {
                                            LOG.error("Bulk write error handler abort exception caught with underlying cause:", cause);
                                        }
                                        result = HandlerResult.ABORT;
                                        error = ahe.getMessage();
                                    } catch (Exception e) {
                                        throw new EsHadoopException("Encountered exception during error handler.", e);
                                    }

                                    switch (result) {
                                        case HANDLED:
                                            Assert.isTrue(errorCollector.getAndClearMessage() == null,
                                                    "Found pass message with Handled response. Be sure to return the value " +
                                                            "returned from pass(String) call.");
                                            // Check for document retries
                                            if (errorCollector.receivedRetries()) {
                                                byte[] retryDataBuffer = errorCollector.getAndClearRetryValue();
                                                if (retryDataBuffer == null || document.bytes() == retryDataBuffer) {
                                                    // Retry the same data.
                                                    // Continue to track the previous attempts.
                                                    retries.add(previousAttempt);
                                                    trackingBytesPosition++;
                                                } else {
                                                    // Check document contents to see if it was deserialized and reserialized.
                                                    if (ArrayUtils.sliceEquals(document.bytes(), document.offset(), document.length(), retryDataBuffer, 0, retryDataBuffer.length)) {
                                                        // Same document content. Leave the data as is in tracking buffer,
                                                        // and continue tracking previous attempts.
                                                        retries.add(previousAttempt);
                                                        trackingBytesPosition++;
                                                    } else {
                                                        // Document has changed.
                                                        // Track new attempts.
                                                        BytesRef newEntry = validateEditedEntry(retryDataBuffer);
                                                        data.remove(trackingBytesPosition);
                                                        data.copyFrom(newEntry);
                                                        // Determine if our tracking bytes array is going to expand.
                                                        if (ba.available() < newEntry.length()) {
                                                            trackingArrayExpanded = true;
                                                        }
                                                        previousAttempt.attemptNumber = 0;
                                                        newDocumentRetries.add(previousAttempt);
                                                    }
                                                }
                                            } else {
                                                // Handled but not retried means we won't have sent that document.
                                                data.remove(trackingBytesPosition);
                                                docsSkipped += 1;
                                            }
                                            break handlerLoop;
                                        case PASS:
                                            String reason = errorCollector.getAndClearMessage();
                                            if (reason != null) {
                                                bulkErrorPassReasons.add(reason);
                                            }
                                            continue handlerLoop;
                                        case ABORT:
                                            errorCollector.getAndClearMessage(); // Sanity clearing
                                            data.remove(trackingBytesPosition);
                                            docsAborted += 1;
                                            abortErrors.add(new BulkResponse.BulkError(previousAttempt.originalPosition, document, status, error));
                                            break handlerLoop;
                                    }
                                }
                            }
                            documentNumber++;
                        }

                        // Place any new documents that have been added at the end of the data buffer at the end of the retry list.
                        retries.addAll(newDocumentRetries);

                        if (!retries.isEmpty()) {
                            retryOperation = true;
                            waitTime = errorCollector.getDelayTimeBetweenRetries();
                        } else {
                            retryOperation = false;
                            if (docsAborted > 0) {
                                bulkResult = BulkResponse.partial(bar.getResponseCode(), bar.getTimeSpent(), totalDocs, docsSent, docsSkipped, docsAborted, abortErrors);
                            } else {
                                bulkResult = BulkResponse.complete(bar.getResponseCode(), bar.getTimeSpent(), totalDocs, docsSent, docsSkipped);
                            }
                        }
                    }
                } while (retryOperation);
            } else {
                bulkResult = BulkResponse.complete();
            }
        } catch (EsHadoopException ex) {
            hadWriteErrors = true;
            throw ex;
        }

        // always discard data since there's no code path that uses the in flight data
        // during retry operations, the tracking bytes array may grow. In that case, do a hard reset.
        // TODO: Perhaps open an issue to limit the expansion of a single byte array (for repeated rewrite-retries)
        if (trackingArrayExpanded) {
            ba = new BytesArray(new byte[settings.getBatchSizeInBytes()], 0);
            data = new TrackingBytesArray(ba);
        } else {
            data.reset();
            dataEntries = 0;
        }

        return bulkResult;
    }

    /**
     * Validate the byte contents of a bulk entry that has been edited before being submitted for retry.
     * @param retryDataBuffer The new entry contents
     * @return A BytesRef that contains the entry contents, potentially cleaned up.
     * @throws EsHadoopIllegalArgumentException In the event that the document data cannot be simply cleaned up.
     */
    private BytesRef validateEditedEntry(byte[] retryDataBuffer) {
        BytesRef result = new BytesRef();

        byte closeBrace = '}';
        byte newline = '\n';

        int newlines = 0;
        for (byte b : retryDataBuffer) {
            if (b == newline) {
                newlines++;
            }
        }

        result.add(retryDataBuffer);

        // Check to make sure that either the last byte is a closed brace or a new line.
        byte lastByte = retryDataBuffer[retryDataBuffer.length - 1];
        if (lastByte == newline) {
            // If last byte is a newline, make sure there are two newlines present in the data
            if (newlines != 2) {
                throw new EsHadoopIllegalArgumentException("Encountered malformed data entry for bulk write retry. " +
                        "Data contains [" + newlines + "] newline characters (\\n) but expected to have [2].");
            }
        } else if (lastByte == closeBrace) {
            // If the last byte is a closed brace, make sure there is only one newline in the data
            if (newlines != 1) {
                throw new EsHadoopIllegalArgumentException("Encountered malformed data entry for bulk write retry. " +
                        "Data contains [" + newlines + "] newline characters (\\n) but expected to have [1].");
            }

            // Add a newline to the entry in this case.
            byte[] trailingNewline = new byte[]{newline};
            result.add(trailingNewline);
        }
        // Further checks are probably intrusive to performance
        return result;
    }

    /**
     * Logs flushing messages and performs backoff waiting if there is a wait time for retry.
     */
    private void initFlushOperation(boolean retryOperation, long waitTime) {
        if (retryOperation) {
            if (waitTime > 0L) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(String.format("Retrying failed bulk documents after backing off for [%s] ms",
                            TimeValue.timeValueMillis(waitTime)));
                }
                try {
                    Thread.sleep(waitTime);
                } catch (InterruptedException e) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Thread interrupted - giving up on retrying...");
                    }
                    throw new EsHadoopException("Thread interrupted - giving up on retrying...", e);
                }
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Retrying failed bulk documents immediately (without backoff)");
                }
            }
        } else if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Sending batch of [%d] bytes/[%s] entries", data.length(), dataEntries));
        }
    }

    /**
     * Attempts a flush operation, handling failed documents based on configured error listeners.
     * @throws EsHadoopException in the event that the bulk operation fails, is aborted, or its errors could not be handled.
     */
    public void flush() {
        BulkResponse bulk = tryFlush();
        if (!bulk.getDocumentErrors().isEmpty()) {
            int maxErrors = 5;
            String header = String.format("Could not write all entries for bulk operation [%s/%s]. Error " +
                    "sample (first [%s] error messages):\n", bulk.getDocumentErrors().size(), bulk.getTotalDocs(), maxErrors);
            StringBuilder message = new StringBuilder(header);
            int i = 0;
            for (BulkResponse.BulkError errors : bulk.getDocumentErrors()) {
                if (i >=maxErrors ) {
                    break;
                }
                message.append("\t").append(errors.getErrorMessage()).append("\n");
                i++;
            }
            message.append("Bailing out...");
            throw new EsHadoopException(message.toString());
        }
    }


    /**
     * Flushes and closes the bulk processor to further writes.
     */
    @Override
    public void close() {
        try {
            if (!hadWriteErrors) {
                flush();
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Dirty close; ignoring last existing write batch...");
                }
            }

            if (requiresRefreshAfterBulk && executedBulkWrite) {
                // refresh batch
                restClient.refresh(resource);

                if (LOG.isDebugEnabled()) {
                    LOG.debug(String.format("Refreshing index [%s]", resource));
                }
            }
        } finally {
            for (BulkWriteErrorHandler handler : documentBulkErrorHandlers) {
                handler.close();
            }
        }
    }

    @Override
    public Stats stats() {
        return new Stats(stats);
    }
}
