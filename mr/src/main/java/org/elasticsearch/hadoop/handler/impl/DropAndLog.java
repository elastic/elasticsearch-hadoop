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

package org.elasticsearch.hadoop.handler.impl;

import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.handler.ErrorCollector;
import org.elasticsearch.hadoop.handler.ErrorHandler;
import org.elasticsearch.hadoop.handler.Exceptional;
import org.elasticsearch.hadoop.handler.HandlerResult;

public class DropAndLog<I extends Exceptional, O, C extends ErrorCollector<O>> implements ErrorHandler<I, O, C> {
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
    private final LogRenderer<I> logLineMaker;

    public static <I extends Exceptional, O, C extends ErrorCollector<O>> DropAndLog<I,O,C> create(LogRenderer<I> logLineMaker) {
        return new DropAndLog<I, O, C>(logLineMaker);
    }

    public DropAndLog(LogRenderer<I> logLineMaker) {
        this.logLineMaker = logLineMaker;
    }

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
    public HandlerResult onError(I entry, C collector) throws Exception {
        switch (loggerLevel) {
            case FATAL:
                if (logger.isFatalEnabled()) {
                    logger.fatal(logLineMaker.renderLog(entry), entry.getException());
                }
                break;
            case ERROR:
                if (logger.isErrorEnabled()) {
                    logger.error(logLineMaker.renderLog(entry), entry.getException());
                }
                break;
            case WARN:
                if (logger.isWarnEnabled()) {
                    logger.warn(logLineMaker.renderLog(entry), entry.getException());
                }
                break;
            case INFO:
                if (logger.isInfoEnabled()) {
                    logger.info(logLineMaker.renderLog(entry), entry.getException());
                }
                break;
            case DEBUG:
                if (logger.isDebugEnabled()) {
                    logger.debug(logLineMaker.renderLog(entry), entry.getException());
                }
                break;
            case TRACE:
                if (logger.isTraceEnabled()) {
                    logger.trace(logLineMaker.renderLog(entry), entry.getException());
                }
                break;
        }
        return HandlerResult.HANDLED;
    }

    @Override
    public void close() {
        // Nothing to do
    }
}
