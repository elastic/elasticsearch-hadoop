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

import java.io.OutputStream;

import org.elasticsearch.hadoop.serialization.json.JacksonJsonGenerator;
import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.FastByteArrayOutputStream;

@SuppressWarnings("rawtypes")
public class ContentBuilder {

    private final Generator generator;
    private final ValueWriter writer;


    private ContentBuilder(Generator generator, ValueWriter writer) {
        Assert.notNull(generator);
        this.generator = generator;
        this.writer = writer;
    }

    public static ContentBuilder generate(ValueWriter writer) {
        return new ContentBuilder(new JacksonJsonGenerator(new FastByteArrayOutputStream()), writer);
    }

    public static ContentBuilder generate(OutputStream bos, ValueWriter writer) {
        return new ContentBuilder(new JacksonJsonGenerator(bos), writer);
    }

    public ContentBuilder nullValue() {
        generator.writeNull();
        return this;
    }

    public ContentBuilder field(String name, String value) {
        field(name);
        if (value == null) {
            generator.writeNull();
        }
        else {
            generator.writeString(value);
        }

        return this;
    }

    public ContentBuilder field(String name, byte[] value) {
        field(name);
        generator.writeBinary(value);
        return this;
    }

    public ContentBuilder field(String name, byte[] value, int offset, int len) {
        field(name);
        generator.writeBinary(value, offset, len);
        return this;
    }

    public ContentBuilder field(String name, boolean value) {
        field(name);
        generator.writeBoolean(value);
        return this;
    }

    public ContentBuilder field(String name, Boolean value) {
        field(name);
        if (value == null) {
            generator.writeNull();
        }
        else {
            generator.writeBoolean(value.booleanValue());
        }

        return this;
    }

    public ContentBuilder field(String name, short value) {
        field(name);
        generator.writeNumber(value);
        return this;
    }

    public ContentBuilder field(String name, Short value) {
        field(name);
        if (value == null) {
            generator.writeNull();
        }
        else {
            generator.writeNumber(value.shortValue());
        }
        return this;
    }

    public ContentBuilder field(String name, byte value) {
        field(name);
        generator.writeNumber(value);
        return this;
    }

    public ContentBuilder field(String name, Byte value) {
        field(name);
        if (value == null) {
            generator.writeNull();
        }
        else {
            generator.writeNumber(value.byteValue());
        }
        return this;
    }

    public ContentBuilder field(String name, int value) {
        field(name);
        generator.writeNumber(value);
        return this;
    }


    public ContentBuilder field(String name, Integer value) {
        field(name);
        if (value == null) {
            generator.writeNull();
        }
        else {
            generator.writeNumber(value.intValue());
        }
        return this;
    }

    public ContentBuilder field(String name, long value) {
        field(name);
        generator.writeNumber(value);
        return this;
    }

    public ContentBuilder field(String name, Long value) {
        field(name);
        if (value == null) {
            generator.writeNull();
        }
        else {
            generator.writeNumber(value.longValue());
        }
        return this;
    }

    public ContentBuilder field(String name, float value) {
        field(name);
        generator.writeNumber(value);
        return this;
    }

    public ContentBuilder field(String name, Float value) {
        field(name);
        if (value == null) {
            generator.writeNull();
        }
        else {
            generator.writeNumber(value.floatValue());
        }
        return this;
    }

    public ContentBuilder field(String name, double value) {
        field(name);
        generator.writeNumber(value);
        return this;
    }

    public ContentBuilder field(String name, Double value) {
        field(name);
        if (value == null) {
            generator.writeNull();
        }
        else {
            generator.writeNumber(value.doubleValue());
        }
        return this;
    }

    public ContentBuilder field(String name, Iterable<?> value) {
        startArray(name);
        for (Object o : value) {
            value(o);
        }
        endArray();
        return this;
    }

    public ContentBuilder field(String name, Object... value) {
        startArray(name);
        for (Object o : value) {
            value(o);
        }
        endArray();
        return this;
    }

    public ContentBuilder startObject() {
        generator.writeBeginObject();
        return this;
    }

    public ContentBuilder endObject() {
        generator.writeEndObject();
        return this;
    }

    public ContentBuilder startArray() {
        generator.writeBeginArray();
        return this;
    }

    public ContentBuilder endArray() {
        generator.writeEndArray();
        return this;
    }

    public ContentBuilder startArray(String name) {
        field(name);
        startArray();
        return this;
    }

    public ContentBuilder field(String name) {
        generator.writeFieldName(name);
        return this;
    }


    public ContentBuilder field(String name, Object value) {
        field(name);
        value(value);
        return this;
    }

    @SuppressWarnings("unchecked")
    public ContentBuilder value(Object value) {
        if (!writer.write(value, generator)) {
            throw new SerializationException(String.format("Cannot handle type [%s], instance [%s] using writer [%s]", value.getClass(), value, writer));
        }
        return this;
    }

    public ContentBuilder flush() {
        generator.flush();
        return this;
    }

    public OutputStream content() {
        return (OutputStream) generator.getOutputTarget();
    }

    public void close() {
        generator.close();
    }
}