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

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.bind.DatatypeConverter;

/**
 * Value writer for JDK types.
 */
public class JdkValueWriter implements ValueWriter<Object> {

    protected final boolean writeUnknownTypes;

    public JdkValueWriter() {
        writeUnknownTypes = false;
    }

    public JdkValueWriter(boolean writeUnknownTypes) {
        this.writeUnknownTypes = writeUnknownTypes;
    }

    @Override
    public boolean write(Object value, Generator generator) {
        if (value == null) {
            generator.writeNull();
        }
        else if (value instanceof String) {
            generator.writeString((String) value);
        }
        else if (value instanceof Integer) {
            generator.writeNumber(((Integer) value).intValue());
        }
        else if (value instanceof Long) {
            generator.writeNumber(((Long) value).longValue());
        }
        else if (value instanceof Float) {
            generator.writeNumber(((Float) value).floatValue());
        }
        else if (value instanceof Double) {
            generator.writeNumber(((Double) value).doubleValue());
        }
        else if (value instanceof Short) {
            generator.writeNumber(((Short) value).shortValue());
        }
        else if (value instanceof Byte) {
            generator.writeNumber(((Byte) value).byteValue());
        }
        // Big Decimal/Integer
        else if (value instanceof Number) {
            generator.writeString(value.toString());
        }
        else if (value instanceof Boolean) {
            generator.writeBoolean(((Boolean) value).booleanValue());
        }
        else if (value instanceof byte[]) {
            generator.writeBinary((byte[]) value);
        }
        else if (value instanceof Object[]) {
            generator.writeBeginArray();
            for (Object o : (Object[]) value) {
                write(o, generator);
            }
            generator.writeEndArray();
        }
        else if (value instanceof Map) {
            generator.writeBeginObject();
            for (Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                generator.writeFieldName(entry.getKey().toString());
                write(entry.getValue(), generator);
            }
            generator.writeEndObject();
        }
        else if (value instanceof Iterable) {
            generator.writeBeginArray();
            for (Object o : (Iterable<?>) value) {
                write(o, generator);
            }
            generator.writeEndArray();
        }
        else if (value instanceof Date) {
            Calendar cal = Calendar.getInstance();
            cal.setTime((Date) value);
            generator.writeString(DatatypeConverter.printDateTime(cal));
        }
        else if (value instanceof Calendar) {
            generator.writeString(DatatypeConverter.printDateTime((Calendar) value));
        }
        else if (value instanceof Timestamp) {
            Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis(((Timestamp) value).getTime());
            generator.writeString(DatatypeConverter.printDateTime(cal));
        }
        else {
            if (writeUnknownTypes) {
                return handleUnknown(value, generator);
            }
            return false;
        }
        return true;
    }

    protected boolean handleUnknown(Object value, Generator generator) {
        generator.writeString(value.toString());
        return true;
    }
}
