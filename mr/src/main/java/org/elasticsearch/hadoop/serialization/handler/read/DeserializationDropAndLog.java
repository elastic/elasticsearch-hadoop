/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.hadoop.serialization.handler.read;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.handler.ErrorCollector;
import org.elasticsearch.hadoop.handler.HandlerResult;
import org.elasticsearch.hadoop.util.FastByteArrayInputStream;

public class DeserializationDropAndLog extends DeserializationErrorHandler {
    public static final String CONF_LOGGER_NAME = "logger.name";
    public static final String CONF_LOGGER_CLASS = "logger.class";
    public static final String CONF_LOGGER_LEVEL = "logger.level";

    private enum LogLevel {
        FATAL,
        ERROR,
        WARN,
        INFO,
        DEBUG,
        TRACE;

        private static Set<String> names = new LinkedHashSet<String>();
        static {
            for (LogLevel logLevel : values()) {
                names.add(logLevel.name());
            }
        }
    }

    private Log logger;
    private LogLevel loggerLevel;

    @Override
    public void init(Properties properties) {
        String loggerName = properties.getProperty(CONF_LOGGER_NAME);
        String loggerClassName = properties.getProperty(CONF_LOGGER_CLASS);
        Class loggerClass;
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

        String rawLoggerLevel = properties.getProperty(CONF_LOGGER_LEVEL, LogLevel.WARN.name());
        if (!LogLevel.names.contains(rawLoggerLevel)) {
            throw new EsHadoopIllegalArgumentException("Invalid logger level [" + rawLoggerLevel + "] given. Available logging levels: " + LogLevel.names.toString());
        }
        loggerLevel = LogLevel.valueOf(rawLoggerLevel);
    }

    @Override
    public HandlerResult onError(DeserializationFailure entry, ErrorCollector<byte[]> collector) throws Exception {
        switch (loggerLevel) {
            case FATAL:
                if (logger.isFatalEnabled()) {
                    logger.fatal(renderLogMessage(entry));
                }
                break;
            case ERROR:
                if (logger.isErrorEnabled()) {
                    logger.error(renderLogMessage(entry));
                }
                break;
            case WARN:
                if (logger.isWarnEnabled()) {
                    logger.warn(renderLogMessage(entry));
                }
                break;
            case INFO:
                if (logger.isInfoEnabled()) {
                    logger.info(renderLogMessage(entry));
                }
                break;
            case DEBUG:
                if (logger.isDebugEnabled()) {
                    logger.debug(renderLogMessage(entry));
                }
                break;
            case TRACE:
                if (logger.isTraceEnabled()) {
                    logger.trace(renderLogMessage(entry));
                }
                break;
        }
        return HandlerResult.HANDLED;
    }

    private String renderLogMessage(DeserializationFailure entry) {
        // Render the previous handler messages
        List<String> handlerMessages = entry.previousHandlerMessages();
        String tailMessage;
        if (!handlerMessages.isEmpty()) {
            StringBuilder tail = new StringBuilder("Previous handler messages:");
            for (String handlerMessage : handlerMessages) {
                tail.append("\n\t").append(handlerMessage);
            }
            tailMessage = tail.toString();
        } else {
            tailMessage = "";
        }

        // Log the failure
        return String.format(
                "Dropping failed bulk entry (response [%s] from server) after [%s] attempts due to error [%s]:%n" +
                        "Entry Contents:%n" +
                        "%s" +
                        "%s",
                entry.getException().getMessage(),
                ((FastByteArrayInputStream) entry.getHitContents()).bytes().toString(),
                tailMessage
        );
    }
}
