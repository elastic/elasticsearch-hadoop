package org.elasticsearch.hadoop.rest.handler;

import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.hadoop.rest.HttpStatus;

/**
 * Created by james.baiera on 11/8/17.
 */
public class ExampleHandler implements WriteErrorHandler {

    private static final Log errorLog = LogFactory.getLog("bulkerror");

    private static final String CONF_KEY = "some.configuration.key";

    private String localState;

    @Override
    public void init(Properties properties) {
        // Initialize handler state using properties from job configuration
        localState = properties.getProperty(CONF_KEY, "default value");
    }

    public HandlerResult onError(BulkWriteFailure entry, ErrorCollector collector) {
        if (entry.getResponseCode() == HttpStatus.CONFLICT) {
            errorLog.error(entry.toString(), entry.getReason());
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
