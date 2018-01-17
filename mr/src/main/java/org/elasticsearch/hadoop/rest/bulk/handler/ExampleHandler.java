package org.elasticsearch.hadoop.rest.bulk.handler;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.hadoop.handler.EsHadoopAbortHandlerException;
import org.elasticsearch.hadoop.handler.HandlerResult;
import org.elasticsearch.hadoop.rest.HttpStatus;

/**
 * EXAMPLE - Simple example error handler that logs and drops conflicts, retries on overload, and otherwise passes
 * evaluation on to the next handler.
 */
public class ExampleHandler extends BulkWriteErrorHandler {

    private static final Log errorLog = LogFactory.getLog("bulk.error");

    private static final String CONF_KEY = "logging.prefix";

    private String loggingPrefix;

    @Override
    public void init(Properties properties) {
        // Initialize handler state using properties from job configuration
        loggingPrefix = properties.getProperty(CONF_KEY, "default value");
    }

    @Override
    public HandlerResult onError(BulkWriteFailure entry, DelayableErrorCollector<byte[]> collector) {
        try {
            if (entry.getResponseCode() == HttpStatus.CONFLICT) {
                String document = readStream(entry.getEntryContents());
                errorLog.error(loggingPrefix + ": DROPPING ["+document+"] due to CONFLICT response.", entry.getException());
                return HandlerResult.HANDLED;
            } else if (entry.getResponseCode() == HttpStatus.TOO_MANY_REQUESTS) {
                return collector.retry();
            }
            return HandlerResult.PASS;
        } catch (IOException ioe) {
            throw new EsHadoopAbortHandlerException("Encountered error: " + ioe.getMessage());
        }
    }

    private String readStream(InputStream entryContents) throws IOException {
        int len = entryContents.available();
        byte[] data = new byte[len];
        entryContents.read(data);
        return new String(data);
    }

    @Override
    public void close() {
        // Clean up some resources if you need to.
    }
}
