package org.elasticsearch.hadoop.rest.bulk;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.hadoop.EsHadoopException;
import org.elasticsearch.hadoop.EsHadoopIllegalStateException;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.handler.EsHadoopAbortHandlerException;
import org.elasticsearch.hadoop.handler.HandlerResult;
import org.elasticsearch.hadoop.rest.BulkResponse;
import org.elasticsearch.hadoop.rest.ErrorExtractor;
import org.elasticsearch.hadoop.rest.Resource;
import org.elasticsearch.hadoop.rest.RestClient;
import org.elasticsearch.hadoop.rest.handler.BulkWriteErrorCollector;
import org.elasticsearch.hadoop.rest.handler.BulkWriteErrorHandler;
import org.elasticsearch.hadoop.rest.handler.BulkWriteFailure;
import org.elasticsearch.hadoop.rest.handler.impl.BulkWriteHandlerLoader;
import org.elasticsearch.hadoop.rest.handler.impl.HttpRetryHandler;
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
        // FIXHERE: If a retry is performed with new data, this byte array will potentially grow in size.
        // Maybe track contents differently or just hard reset the byte array if it grew.
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
                boolean firstRun = true;
                boolean retryOperation = false;
                long waitTime = 0L;
                List<Integer> attempts = Collections.emptyList();
                List<Integer> originalPositions = new ArrayList<Integer>();
                List<BulkResponse.BulkError> abortErrors = new ArrayList<BulkResponse.BulkError>();

                do {
                    initFlushOperation(retryOperation, waitTime);

                    // Exec bulk operation to ES, get response.
                    RestClient.BulkActionResponse bar = restClient.bulk(resource, data);

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
                        // If no items on response, assume all documents made it in.
                        // recorded bytes are ack here
                        stats.bytesAccepted += data.length();
                        stats.docsAccepted += data.entries();
                        retryOperation = false;
                        bulkResult = BulkResponse.complete(bar.getResponseCode(), bar.getTimeSpent(), totalDocs, totalDocs, 0);
                    } else {
                        // Keep track of where the doc was originally, and where we are in removing/keeping data.
                        int documentNumber = 0;
                        int trackingBytesPosition = 0;

                        List<Integer> previousAttempts = attempts;
                        List<Integer> previousOriginalPositions = originalPositions;
                        List<Integer> tailOriginalPositions = new ArrayList<Integer>();
                        attempts = new ArrayList<Integer>();
                        originalPositions = new ArrayList<Integer>();
                        BulkWriteErrorCollector errorCollector = new BulkWriteErrorCollector();

                        // Iterate over all entries, and for each error found, attempt to handle the problem.
                        for (Iterator<Map> iterator = bar.getEntries(); iterator.hasNext(); ) {

                            // The array of maps are (operation -> document info) maps:
                            Map map = iterator.next();
                            // get the underlying document information as a map:
                            Map values = (Map) map.values().iterator().next();
                            Integer docStatus = (Integer) values.get("status");
                            String error = errorExtractor.extractError(values);
                            if (error != null && !error.isEmpty()) {
                                // Found failed write
                                BytesArray document = data.entry(trackingBytesPosition);
                                int status = docStatus == null ? -1 : docStatus; // In pre-2.x ES versions, the status is not included.

                                // Figure out which attempt number sending this document was.
                                int requestAttempt;
                                if (previousAttempts.isEmpty() || documentNumber >= previousAttempts.size()) {
                                    // We don't have an attempt, assume first attempt
                                    requestAttempt = 1;
                                } else {
                                    // Get and increment previous attempt value
                                    requestAttempt = previousAttempts.get(documentNumber) + 1;
                                }

                                // Figure out which position the doc was in
                                int originalPosition;
                                if (firstRun) {
                                    originalPosition = documentNumber;
                                } else {
                                    originalPosition = previousOriginalPositions.get(documentNumber);
                                }

                                // Handle bulk write failures
                                List<String> bulkErrorPassReasons = new ArrayList<String>();
                                BulkWriteFailure failure = new BulkWriteFailure(
                                        status,
                                        new Exception(error),
                                        document,
                                        requestAttempt,
                                        bulkErrorPassReasons
                                );

                                for (BulkWriteErrorHandler errorHandler : documentBulkErrorHandlers) {
                                    HandlerResult result;
                                    try {
                                        result = errorHandler.onError(failure, errorCollector);
                                    } catch (EsHadoopAbortHandlerException ahe) {
                                        throw new EsHadoopException(ahe.getMessage());
                                    } catch (Exception e) {
                                        throw new EsHadoopException("Encountered exception during error handler. Treating " +
                                                "it as an ABORT result.", e);
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
                                                    attempts.add(requestAttempt);
                                                    originalPositions.add(originalPosition);
                                                    trackingBytesPosition++;
                                                } else {
                                                    // Check document contents to see if it was deserialized and reserialized.
                                                    if (ArrayUtils.sliceEquals(document.bytes(), document.offset(), document.length(), retryDataBuffer, 0, retryDataBuffer.length)) {
                                                        // Same document content. Leave the data as is in tracking buffer,
                                                        // and continue tracking previous attempts.
                                                        attempts.add(requestAttempt);
                                                        originalPositions.add(originalPosition);
                                                        trackingBytesPosition++;
                                                    } else {
                                                        // Document has changed.
                                                        // Track new attempts.
                                                        data.remove(trackingBytesPosition);
                                                        data.copyFrom(new BytesArray(retryDataBuffer));
                                                        trackingArrayExpanded = true;
                                                        tailOriginalPositions.add(originalPosition);
                                                        // Don't add item to attempts. When the attempts list is exhausted we'll assume attempt values of 1.
                                                    }
                                                }
                                            } else {
                                                // Handled but not retried means we won't have sent that document.
                                                data.remove(trackingBytesPosition);
                                                docsSkipped += 1;
                                            }
                                            break;
                                        case PASS:
                                            String reason = errorCollector.getAndClearMessage();
                                            if (reason != null) {
                                                bulkErrorPassReasons.add(reason);
                                            }
                                            continue;
                                        case ABORT:
                                            errorCollector.getAndClearMessage(); // Sanity clearing
                                            data.remove(trackingBytesPosition);
                                            docsAborted += 1;
                                            abortErrors.add(new BulkResponse.BulkError(originalPosition, document, status, error));
                                    }
                                }
                            } else {
                                stats.bytesAccepted += data.length(trackingBytesPosition);
                                stats.docsAccepted += 1;
                                docsSent += 1;
                                data.remove(trackingBytesPosition);
                            }
                            documentNumber++;
                        }

                        if (!attempts.isEmpty()) {
                            retryOperation = true;
                            firstRun = false;
                            waitTime = errorCollector.getDelayTimeBetweenRetries();
                            originalPositions.addAll(tailOriginalPositions);
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
//            "Error Handler returned an ABORT result for failed " +
//            "bulk document. HTTP Status [" + status +
//            "], Error Message [" + error + "], Document Entry [" +
//            document.toString() + "]"
            String header = String.format("Could not write all entries [%s/%s] (Maybe ES was overloaded?). Error " +
                    "sample (first [%s] error messages):\n", bulk.getDocumentErrors().size(), bulk.getTotalDocs(), 5);
            StringBuilder message = new StringBuilder(header);
            int i = 0;
            for (BulkResponse.BulkError errors : bulk.getDocumentErrors()) {
                if (i >=5 ) {
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
