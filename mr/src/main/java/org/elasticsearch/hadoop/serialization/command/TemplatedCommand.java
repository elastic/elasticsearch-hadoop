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
package org.elasticsearch.hadoop.serialization.command;

import java.util.Collection;

import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.serialization.builder.ContentBuilder;
import org.elasticsearch.hadoop.serialization.builder.ValueWriter;
import org.elasticsearch.hadoop.serialization.field.FieldExtractor;
import org.elasticsearch.hadoop.util.BytesArray;
import org.elasticsearch.hadoop.util.BytesRef;
import org.elasticsearch.hadoop.util.FastByteArrayOutputStream;

class TemplatedCommand implements Command {

    static class FieldWriter {
        final FieldExtractor extractor;
        final BytesArray pad;

        FieldWriter(FieldExtractor extractor) {
            this(extractor, new BytesArray(64));
        }

        FieldWriter(FieldExtractor extractor, BytesArray pad) {
            this.extractor = extractor;
            this.pad = pad;
        }

        BytesArray write(Object object) {
            String value = extractor.field(object);
            if (value == null) {
                throw new EsHadoopIllegalArgumentException(String.format("[%s] cannot extract value from object [%s]", extractor, object));
            }
            pad.bytes(value);
            return pad;
        }
    }

    private final Collection<Object> beforeObject;
    private final Collection<Object> afterObject;

    private BytesArray scratchPad = new BytesArray(1024);
    private BytesRef ref = new BytesRef();

    private final ValueWriter<?> valueWriter;

    TemplatedCommand(Collection<Object> beforeObject, Collection<Object> afterObject, ValueWriter<?> valueWriter) {
        this.beforeObject = beforeObject;
        this.afterObject = afterObject;
        this.valueWriter = valueWriter;
    }

    @Override
    public BytesRef write(Object object) {
        ref.reset();
        scratchPad.reset();

        Object processed = preProcess(object, scratchPad);
        // write before object
        writeTemplate(beforeObject, processed);
        // write object
        doWriteObject(processed, scratchPad, valueWriter);
        ref.add(scratchPad);
        // writer after object
        writeTemplate(afterObject, processed);
        return ref;
    }

    protected Object preProcess(Object object, BytesArray storage) {
        return object;
    }

    protected void doWriteObject(Object object, BytesArray storage, ValueWriter<?> writer) {
        FastByteArrayOutputStream bos = new FastByteArrayOutputStream(storage);
        ContentBuilder.generate(bos, writer).value(object).flush().close();
    }

    private void writeTemplate(Collection<Object> template, Object object) {
        for (Object item : template) {
            if (item instanceof byte[]) {
                ref.add((byte[]) item);
            }
            else {
                ref.add(((FieldWriter) item).write(object));
            }
        }
    }
}