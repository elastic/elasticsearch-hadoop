package org.elasticsearch.hadoop.rest.handler;

import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.hadoop.handler.ErrorCollector;
import org.elasticsearch.hadoop.handler.HandlerResult;
import org.elasticsearch.hadoop.rest.HttpStatus;

/**
 * EXAMPLE - Simple example error handler that logs and drops conflicts, retries on overload, and otherwise passes
 * evaluation on to the next handler.
 */
public class ExampleHandler extends BulkWriteErrorHandler {

    private static final Log errorLog = LogFactory.getLog("bulk.error");

    private static final String CONF_KEY = "some.configuration.key";

    private String localState;

    @Override
    public void init(Properties properties) {
        // Initialize handler state using properties from job configuration
        localState = properties.getProperty(CONF_KEY, "default value");
    }

    @Override
    public HandlerResult onError(BulkWriteFailure entry, ErrorCollector<byte[]> collector) {
        if (entry.getResponseCode() == HttpStatus.CONFLICT) {
            errorLog.error("DROPPING ["+entry.toString()+"] due to CONFLICT response.", entry.getReason());
            return HandlerResult.HANDLED;
        } else if (entry.getResponseCode() == HttpStatus.TOO_MANY_REQUESTS) {
            return collector.retry(entry.getEntryContents());
        }
        return HandlerResult.PASS;
    }

    @Override
    public void close() {
        // Clean up some resources if you need to.
        localState = "close something etc...";
    }
}
