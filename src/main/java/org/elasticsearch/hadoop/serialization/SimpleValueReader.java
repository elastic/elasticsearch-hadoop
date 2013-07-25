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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.hadoop.serialization.Parser.NumberType;
import org.elasticsearch.hadoop.serialization.Parser.Token;
import org.elasticsearch.hadoop.util.Assert;


/**
 * Basic value reader handling using the implied JSON type.
 */
public class SimpleValueReader implements FieldReader {

    @Override
    public Object readValue(Parser parser, String value, FieldType esType) {

        switch (esType) {
        case NULL:
            return nullValue(value);
        case STRING:
            return textValue(value);
        case INTEGER:
            return intValue(value);
        case LONG:
            return longValue(value);
        case FLOAT:
            return floatValue(value);
        case DOUBLE:
            return doubleValue(value);
        case BOOLEAN:
            return booleanValue(value);
        case BINARY:
            return binaryValue(parser.binaryValue());
        case DATE:
            return date(value);
        case IP:
            throw new UnsupportedOperationException("not implemented yet");
        case OBJECT:
            throw new UnsupportedOperationException("not implemented yet");
        }
        return null;
    }

    @Override
    public Map createMap() {
        return new LinkedHashMap<Object, Object>();
    }

    @Override
    public void addToMap(Object map, Object key, Object value) {
        ((Map) map).put(key, value);
    }

    @Override
    public Object createArray(FieldType type) {
        return new ArrayList<Object>();
    }

    @Override
    public void addToArray(Object array, List<Object> value) {
        ((List) array).addAll(value);
    }

    protected Object binaryValue(byte[] value) {
        return value;
    }

    protected Object booleanValue(String value) {
        return Boolean.parseBoolean(value);
    }

    protected Object doubleValue(String value) {
        return Double.parseDouble(value);
    }

    protected Object floatValue(String value) {
        return Float.parseFloat(value);
    }

    protected Object longValue(String value) {
        return Long.parseLong(value);
    }

    protected Object intValue(String value) {
        return Integer.parseInt(value);
    }

    protected Object textValue(String value) {
        return value;
    }

    protected Object nullValue(String value) {
        return null;
    }

    private Object date(String value) {
        throw new UnsupportedOperationException("wip");
    }
}