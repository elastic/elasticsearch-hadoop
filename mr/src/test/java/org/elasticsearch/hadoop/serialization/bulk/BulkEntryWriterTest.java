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

import org.elasticsearch.hadoop.EsHadoopException;
import org.elasticsearch.hadoop.EsHadoopIllegalStateException;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.handler.ErrorCollector;
import org.elasticsearch.hadoop.handler.EsHadoopAbortHandlerException;
import org.elasticsearch.hadoop.handler.HandlerResult;
import org.elasticsearch.hadoop.serialization.EsHadoopSerializationException;
import org.elasticsearch.hadoop.serialization.handler.write.SerializationErrorHandler;
import org.elasticsearch.hadoop.serialization.handler.write.SerializationFailure;
import org.elasticsearch.hadoop.serialization.handler.write.impl.SerializationHandlerLoader;
import org.elasticsearch.hadoop.util.BytesRef;
import org.elasticsearch.hadoop.util.TestSettings;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.fail;

public class BulkEntryWriterTest {

    @Test(expected = EsHadoopSerializationException.class)
    public void testWriteBulkEntryThatAbortsByDefault() {
        BulkCommand command = Mockito.mock(BulkCommand.class);
        Mockito.when(command.write(1)).thenThrow(new EsHadoopIllegalStateException("Things broke"));

        Settings settings = new TestSettings();

        BulkEntryWriter bulkEntryWriter = new BulkEntryWriter(settings, command);

        bulkEntryWriter.writeBulkEntry(1);
        fail("Should abort on error by default");
    }

    @Test(expected = EsHadoopException.class)
    public void testWriteBulkEntryWithHandlerThrowingException() {
        BulkCommand command = Mockito.mock(BulkCommand.class);
        Mockito.when(command.write(1)).thenThrow(new EsHadoopIllegalStateException("Things broke"));

        Settings settings = new TestSettings();
        settings.setProperty(SerializationHandlerLoader.ES_WRITE_DATA_ERROR_HANDLERS, "thrower");
        settings.setProperty(SerializationHandlerLoader.ES_WRITE_DATA_ERROR_HANDLER+".thrower", ExceptionThrowingHandler.class.getName());

        BulkEntryWriter bulkEntryWriter = new BulkEntryWriter(settings, command);

        bulkEntryWriter.writeBulkEntry(1);
        fail("Should fail from unexpected handler exception");
    }

    @Test(expected = EsHadoopSerializationException.class)
    public void testWriteBulkEntryWithHandlerThrowingAbortException() {
        BulkCommand command = Mockito.mock(BulkCommand.class);
        Mockito.when(command.write(1)).thenThrow(new EsHadoopIllegalStateException("Things broke"));

        Settings settings = new TestSettings();
        settings.setProperty(SerializationHandlerLoader.ES_WRITE_DATA_ERROR_HANDLERS, "thrower");
        settings.setProperty(SerializationHandlerLoader.ES_WRITE_DATA_ERROR_HANDLER+".thrower", AbortingExceptionThrowingHandler.class.getName());

        BulkEntryWriter bulkEntryWriter = new BulkEntryWriter(settings, command);

        bulkEntryWriter.writeBulkEntry(1);
        fail("Should fail from aborting handler exception");
    }

    @Test(expected = EsHadoopException.class)
    public void testWriteBulkEntryWithNeverEndingHandler() {
        BulkCommand command = Mockito.mock(BulkCommand.class);
        Mockito.when(command.write(1)).thenThrow(new EsHadoopIllegalStateException("Things broke"));

        Settings settings = new TestSettings();
        settings.setProperty(SerializationHandlerLoader.ES_WRITE_DATA_ERROR_HANDLERS, "evil");
        settings.setProperty(SerializationHandlerLoader.ES_WRITE_DATA_ERROR_HANDLER+".evil", NeverSurrenderHandler.class.getName());

        BulkEntryWriter bulkEntryWriter = new BulkEntryWriter(settings, command);

        bulkEntryWriter.writeBulkEntry(1);
        fail("Should fail from too many unsuccessful retries");
    }

    @Test
    public void testWriteBulkEntryWithIgnoreFailure() {
        BulkCommand command = Mockito.mock(BulkCommand.class);
        Mockito.when(command.write(1)).thenThrow(new EsHadoopIllegalStateException("Things broke"));

        Settings settings = new TestSettings();
        settings.setProperty(SerializationHandlerLoader.ES_WRITE_DATA_ERROR_HANDLERS, "skip");
        settings.setProperty(SerializationHandlerLoader.ES_WRITE_DATA_ERROR_HANDLER+".skip", NothingToSeeHereHandler.class.getName());

        BulkEntryWriter bulkEntryWriter = new BulkEntryWriter(settings, command);

        BytesRef value = bulkEntryWriter.writeBulkEntry(1);
        Assert.assertNull("Skipped values should be null", value);
    }

    @Test
    public void testWriteBulkEntryWithHandlersThatPassMessages() {
        BulkCommand command = Mockito.mock(BulkCommand.class);
        Mockito.when(command.write(1)).thenThrow(new EsHadoopIllegalStateException("Things broke"));

        Settings settings = new TestSettings();
        settings.setProperty(SerializationHandlerLoader.ES_WRITE_DATA_ERROR_HANDLERS, "marco,polo,skip");
        settings.setProperty(SerializationHandlerLoader.ES_WRITE_DATA_ERROR_HANDLER+".marco", MarcoHandler.class.getName());
        settings.setProperty(SerializationHandlerLoader.ES_WRITE_DATA_ERROR_HANDLER+".polo", PoloHandler.class.getName());
        settings.setProperty(SerializationHandlerLoader.ES_WRITE_DATA_ERROR_HANDLER+".skip", NothingToSeeHereHandler.class.getName());

        BulkEntryWriter bulkEntryWriter = new BulkEntryWriter(settings, command);

        BytesRef value = bulkEntryWriter.writeBulkEntry(1);
        Assert.assertNull("Skipped values should be null", value);
    }

    @Test
    public void testWriteBulkEntryWithHandlersThatCorrectsData() {
        BytesRef response = new BytesRef();
        response.add("abcdefg".getBytes());
        BulkCommand command = Mockito.mock(BulkCommand.class);
        Mockito.when(command.write(1)).thenThrow(new EsHadoopIllegalStateException("Things broke"));
        Mockito.when(command.write(10)).thenReturn(response);

        Settings settings = new TestSettings();
        settings.setProperty(SerializationHandlerLoader.ES_WRITE_DATA_ERROR_HANDLERS, "fix");
        settings.setProperty(SerializationHandlerLoader.ES_WRITE_DATA_ERROR_HANDLER+".fix", CorrectingHandler.class.getName());

        BulkEntryWriter bulkEntryWriter = new BulkEntryWriter(settings, command);

        BytesRef value = bulkEntryWriter.writeBulkEntry(1);
        Assert.assertNotNull("Skipped values should be null", value);
        Assert.assertEquals(7, response.length());
        Assert.assertArrayEquals("abcdefg".getBytes(), response.toString().getBytes());
    }

    /**
     * Case: Handler throws random Exceptions
     * Outcome: Processing fails fast.
     */
    public static class ExceptionThrowingHandler extends SerializationErrorHandler {
        @Override
        public HandlerResult onError(SerializationFailure entry, ErrorCollector<Object> collector) throws Exception {
            throw new IllegalArgumentException("Whoopsie!");
        }
    }

    /**
     * Case: Handler throws exception, wrapped in abort based exception
     * Outcome: Exception is collected and used as the reason for aborting that specific document.
     */
    public static class AbortingExceptionThrowingHandler extends SerializationErrorHandler {
        @Override
        public HandlerResult onError(SerializationFailure entry, ErrorCollector<Object> collector) throws Exception {
            throw new EsHadoopAbortHandlerException("Abort the handler!!");
        }
    }

    /**
     * Case: Evil or incorrect handler causes infinite loop.
     */
    public static class NeverSurrenderHandler extends SerializationErrorHandler {
        @Override
        public HandlerResult onError(SerializationFailure entry, ErrorCollector<Object> collector) throws Exception {
            return collector.retry(); // NEVER GIVE UP
        }
    }

    /**
     * Case: Handler acks the failure and expects the processing to move along.
     */
    public static class NothingToSeeHereHandler extends SerializationErrorHandler {
        @Override
        public HandlerResult onError(SerializationFailure entry, ErrorCollector<Object> collector) throws Exception {
            return HandlerResult.HANDLED; // Move along.
        }
    }

    /**
     * Case: Handler passes on the failure, setting a "message for why"
     */
    public static class MarcoHandler extends SerializationErrorHandler {
        @Override
        public HandlerResult onError(SerializationFailure entry, ErrorCollector<Object> collector) throws Exception {
            return collector.pass("MARCO!");
        }
    }

    /**
     * Case: Handler checks the pass messages and ensures that they have been set.
     * Outcome: If set, it acks and continues, and if not, it aborts.
     */
    public static class PoloHandler extends SerializationErrorHandler {
        @Override
        public HandlerResult onError(SerializationFailure entry, ErrorCollector<Object> collector) throws Exception {
            if (entry.previousHandlerMessages().contains("MARCO!")) {
                return collector.pass("POLO!");
            }
            throw new EsHadoopAbortHandlerException("FISH OUT OF WATER!");
        }
    }

    /**
     * Case: Handler somehow knows how to fix data.
     * Outcome: Data is deserialized correctly.
     */
    public static class CorrectingHandler extends SerializationErrorHandler {
        @Override
        public HandlerResult onError(SerializationFailure entry, ErrorCollector<Object> collector) throws Exception {
            entry.getException().printStackTrace();
            return collector.retry(10);
        }
    }
}