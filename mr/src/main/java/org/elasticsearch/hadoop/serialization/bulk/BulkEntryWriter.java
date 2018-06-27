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

package org.elasticsearch.hadoop.serialization.bulk;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.hadoop.EsHadoopException;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.handler.EsHadoopAbortHandlerException;
import org.elasticsearch.hadoop.handler.HandlerResult;
import org.elasticsearch.hadoop.serialization.EsHadoopSerializationException;
import org.elasticsearch.hadoop.serialization.handler.SerdeErrorCollector;
import org.elasticsearch.hadoop.serialization.handler.write.ISerializationErrorHandler;
import org.elasticsearch.hadoop.serialization.handler.write.SerializationFailure;
import org.elasticsearch.hadoop.serialization.handler.write.impl.SerializationHandlerLoader;
import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.BytesRef;

/**
 * Manages the creation of bulk entries for use in a bulk request. Encapsulates all serialization error handling
 * logic. Must be closed at conclusion of the process.
 */
public class BulkEntryWriter {

    private static final Log LOG = LogFactory.getLog(BulkEntryWriter.class);

    private final BulkCommand bulkCommand;
    private final List<ISerializationErrorHandler> serializationErrorHandlers;

    public BulkEntryWriter(Settings settings, BulkCommand bulkCommand) {
        this.bulkCommand = bulkCommand;

        SerializationHandlerLoader loader = new SerializationHandlerLoader();
        loader.setSettings(settings);

        this.serializationErrorHandlers = loader.loadHandlers();
    }

    public BytesRef writeBulkEntry(Object object) {
        Object toRead = object;
        BytesRef writeResult = null;
        boolean retryWrite = false;
        boolean skip = false;
        int attempts = 0;
        do {
            try {
                retryWrite = false;
                writeResult = bulkCommand.write(toRead);
            } catch (Exception serializationException) {
                // Create error event
                List<String> passReasons = new ArrayList<String>();
                SerializationFailure entry = new SerializationFailure(serializationException, object, passReasons);

                // Set up error collector
                SerdeErrorCollector<Object> errorCollector = new SerdeErrorCollector<Object>();

                // Attempt failure handling
                Exception abortException = serializationException;
                handlerloop:
                for (ISerializationErrorHandler serializationErrorHandler : serializationErrorHandlers) {
                    HandlerResult result;
                    try {
                        result = serializationErrorHandler.onError(entry, errorCollector);
                    } catch (EsHadoopAbortHandlerException ahe) {
                        // Count this as an abort operation. Wrap cause in a serialization exception.
                        result = HandlerResult.ABORT;
                        abortException = new EsHadoopSerializationException(ahe.getMessage(), ahe.getCause());
                    } catch (Exception e) {
                        LOG.error("Could not handle serialization error event due to an exception in error handler. " +
                                "Serialization exception:", serializationException);
                        throw new EsHadoopException("Encountered unexpected exception during error handler execution.", e);
                    }

                    switch (result) {
                        case HANDLED:
                            Assert.isTrue(errorCollector.getAndClearMessage() == null,
                                    "Found pass message with Handled response. Be sure to return the value returned from " +
                                            "the pass(String) call.");
                            // Check for retries
                            if (errorCollector.receivedRetries()) {
                                Object retryObject = errorCollector.getAndClearRetryValue();
                                if (retryObject != null) {
                                    // Use new retry object to read
                                    toRead = retryObject;
                                }
                                // If null, retry same object.

                                // Limit the number of retries though to like 50
                                if (attempts >= 50) {
                                    throw new EsHadoopException("Maximum retry attempts (50) reached for serialization errors.");
                                } else {
                                    retryWrite = true;
                                    attempts++;
                                }
                            } else {
                                if (LOG.isDebugEnabled()) {
                                    LOG.debug("Skipping a record that resulted in error while reading: [" +
                                            object.toString() + "]");
                                } else {
                                    LOG.info("Skipping a record that resulted in error while reading. (DEBUG for more info).");
                                }
                                skip = true;
                            }
                            break handlerloop;
                        case PASS:
                            String reason = errorCollector.getAndClearMessage();
                            if (reason != null) {
                                passReasons.add(reason);
                            }
                            continue handlerloop;
                        case ABORT:
                            errorCollector.getAndClearMessage(); // Sanity clearing
                            if (abortException instanceof EsHadoopSerializationException) {
                                throw (EsHadoopSerializationException) abortException;
                            } else {
                                throw new EsHadoopSerializationException(abortException);
                            }
                    }
                }
            }
        } while (retryWrite);

        if (writeResult == null && skip == false) {
            throw new EsHadoopSerializationException("Could not write record to bulk request.");
        }

        return writeResult;
    }

    public void close() {
        for (ISerializationErrorHandler errorHandler : serializationErrorHandlers) {
            errorHandler.close();
        }
    }
}
