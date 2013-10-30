/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticsearch.hadoop.serialization;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.util.BytesArray;
import org.elasticsearch.hadoop.util.FastByteArrayOutputStream;
import org.elasticsearch.hadoop.util.ObjectUtils;
import org.elasticsearch.hadoop.util.StringUtils;

/**
 * Base class for Bulk commands.
 */
abstract class AbstractCommand implements Command {

    private static Log log = LogFactory.getLog(BulkCommands.class);

    protected final IdExtractor idExtractor;
    protected final ValueWriter<?> valueWriter;

    private BytesArray scratchPad = new BytesArray(1024);
    private byte[] idAsBytes = null;

    static final byte[] CARRIER_RETURN = "\n".getBytes(StringUtils.UTF_8);

    AbstractCommand(Settings settings) {
        this.valueWriter = ObjectUtils.instantiate(settings.getSerializerValueWriterClassName(), settings);
        this.idExtractor = (StringUtils.hasText(settings.getMappingId()) ? ObjectUtils.<IdExtractor> instantiate(
                settings.getMappingIdExtractorClassName(), settings) : null);

        if (log.isTraceEnabled()) {
            log.trace(String.format("Instantiated value writer [%s]", valueWriter));
            if (idExtractor != null) {
                log.trace(String.format("Instantiated id extractor [%s]", idExtractor));
            }
        }
    }

    @Override
    public int prepare(Object object) {
        int entrySize = 0;

        if (object instanceof SerializedObject) {
            SerializedObject so = ((SerializedObject) object);
            idAsBytes = so.id;
        }

        else {
            String id = extractId(object);
            if (StringUtils.hasText(id)) {
                idAsBytes = id.getBytes(StringUtils.UTF_8);
            }
        }

        if (idAsBytes != null) {
            entrySize += headerPrefix().length;
            entrySize += idAsBytes.length;
            entrySize += headerSuffix().length;
        }
        else {
            if (isIdRequired()) {
                throw new IllegalArgumentException(String.format(
                        "Operation [%s] requires an id but none was given/found", this.toString()));
            }
            else {
                entrySize += header().length;
            }
        }

        if (isSourceRequired()) {
            if (object instanceof SerializedObject) {
                entrySize += ((SerializedObject) object).size;
            }
            else {
                serialize(object);
                // add the trailing \n
                entrySize += scratchPad.size();
            }
            // trailing \n
            entrySize++;
        }

        return entrySize;
    }

    @Override
    public void write(Object object, BytesArray buffer) {
        writeActionAndMetadata(object, buffer);
        if (isSourceRequired()) {
            writeSource(object, buffer);
            writeTrailingReturn(buffer);
        }
    }

    protected void writeActionAndMetadata(Object object, BytesArray data) {
        if (idAsBytes != null) {
            copyIntoBuffer(headerPrefix(), data);
            copyIntoBuffer(idAsBytes, data);
            copyIntoBuffer(headerSuffix(), data);
        }
        else {
            copyIntoBuffer(header(), data);
        }
    }

    private String extractId(Object object) {
        return (idExtractor != null ? idExtractor.id(object) : null);
    }

    protected abstract byte[] headerPrefix();

    protected abstract byte[] headerSuffix();

    protected abstract byte[] header();


    protected boolean isSourceRequired() {
        return true;
    }

    protected void writeSource(Object object, BytesArray buffer) {
        // object was serialized - just write it down
        if (object instanceof SerializedObject) {
            SerializedObject so = (SerializedObject) object;
            System.arraycopy(so.data, 0, buffer.bytes(), buffer.size(), so.size);
            buffer.increment(so.size);
        }
        else {
            System.arraycopy(scratchPad.bytes(), 0, buffer.bytes(), buffer.size(), scratchPad.size());
            buffer.increment(scratchPad.size());
        }
    }

    private void writeTrailingReturn(BytesArray buffer) {
        System.arraycopy(CARRIER_RETURN, 0, buffer.bytes(), buffer.size(), CARRIER_RETURN.length);
        buffer.increment(1);
    }

    private void serialize(Object object) {
        scratchPad.reset();
        FastByteArrayOutputStream bos = new FastByteArrayOutputStream(scratchPad);
        ContentBuilder.generate(bos, valueWriter).value(object).flush().close();
    }

    protected boolean isIdRequired() {
        return false;
    }

    static final void copyIntoBuffer(byte[] content, BytesArray bytes) {
        System.arraycopy(content, 0, bytes.bytes(), bytes.size(), content.length);
        bytes.increment(content.length);
    }
}