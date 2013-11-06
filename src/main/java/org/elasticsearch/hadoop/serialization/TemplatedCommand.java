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

import java.util.Collection;

import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.BytesArray;
import org.elasticsearch.hadoop.util.BytesRef;
import org.elasticsearch.hadoop.util.FastByteArrayOutputStream;
import org.elasticsearch.hadoop.util.StringUtils;

class TemplatedCommand implements Command {

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
        ref.clear();

        // write before object
        writeTemplate(beforeObject, object);
        // write object
        scratchPad.reset();
        FastByteArrayOutputStream bos = new FastByteArrayOutputStream(scratchPad);
        ContentBuilder.generate(bos, valueWriter).value(object).flush().close();
        ref.add(scratchPad);
        // writer after object
        writeTemplate(afterObject, object);
        return ref;
    }

    private void writeTemplate(Collection<Object> template, Object object) {
        for (Object item : template) {
            if (item instanceof byte[]) {
                ref.add((byte[]) item);
            }
            else {
                FieldExtractor extractor = (FieldExtractor) item;
                String value = extractor.field(object);
                Assert.notNull(value, String.format("[%s] cannot extract value from object [%s]", extractor, object));
                ref.add(value.getBytes(StringUtils.UTF_8));
            }
        }
    }
}
