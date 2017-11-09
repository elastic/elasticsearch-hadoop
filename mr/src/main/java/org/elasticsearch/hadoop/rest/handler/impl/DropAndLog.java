package org.elasticsearch.hadoop.rest.handler.impl;

import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.handler.ErrorCollector;
import org.elasticsearch.hadoop.handler.HandlerResult;
import org.elasticsearch.hadoop.rest.handler.BulkWriteErrorHandler;
import org.elasticsearch.hadoop.rest.handler.BulkWriteFailure;

/**
 * Drops and logs any given error messages.
 */
public class DropAndLog extends BulkWriteErrorHandler {

    private final String CONF_LOGGER_NAME = "logger.name";
    private final String CONF_LOGGER_CLASS = "logger.class";

    private String loggerName;
    private Class loggerClass;
    private Log logger;

    @Override
    public void init(Properties properties) {
        loggerName = properties.getProperty(CONF_LOGGER_NAME);
        String loggerClassName = properties.getProperty(CONF_LOGGER_CLASS);
        if (loggerClassName != null) {
            try {
                loggerClass = Class.forName(loggerClassName);
            } catch (ClassNotFoundException cnfe) {
                // Could not find class name
                throw new EsHadoopIllegalArgumentException("Could not locate logger class [" + loggerClassName + "].", cnfe);
            }
        } else {
            loggerClass = null;
        }

        if (loggerName != null && loggerClass != null) {
            throw new EsHadoopIllegalArgumentException("Both logger name and logger class provided for drop and log handler. Provide only one. Bailing out...");
        }

        if (loggerName != null) {
            logger = LogFactory.getLog(loggerName);
        } else if (loggerClass != null) {
            logger = LogFactory.getLog(loggerClass);
        } else {
            throw new EsHadoopIllegalArgumentException("No logger name or logger class provided for drop and log handler. Provide one. Bailing out...");
        }
    }

    @Override
    public HandlerResult onError(BulkWriteFailure entry, ErrorCollector<byte[]> collector) throws Exception {
        logger.warn(entry, entry.getReason());
        return HandlerResult.HANDLED;
    }
}
