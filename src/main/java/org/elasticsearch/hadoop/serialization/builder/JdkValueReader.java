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
package org.elasticsearch.hadoop.serialization.builder;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.serialization.FieldType;
import org.elasticsearch.hadoop.serialization.Parser;
import org.elasticsearch.hadoop.serialization.SettingsAware;
import org.elasticsearch.hadoop.util.StringUtils;


/**
 * Basic value reader handling using the implied JSON type.
 */
public class JdkValueReader implements SettingsAware, ValueReader {

    private boolean emptyAsNull = true;

    @Override
    public Object readValue(Parser parser, String value, FieldType esType) {
        if (esType == null) {
            return null;
        }

        switch (esType) {
        case NULL:
            return nullValue();
        case STRING:
            return textValue(value);
        case INTEGER:
            return intValue(value);
        case TOKEN_COUNT:
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
        case OBJECT:
            throw new UnsupportedOperationException("not implemented yet");
            // everything else (IP, GEO) gets translated to strings
        default:
            return textValue(value);
        }
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Map createMap() {
        return new LinkedHashMap<Object, Object>();
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public void addToMap(Object map, Object key, Object value) {
        ((Map) map).put(key, (value != null ? value : nullValue()));
    }

    @Override
    public Object createArray(FieldType type) {
        // no need to create a collection, we'll just reuse the one passed to #addToArray
        return Collections.emptyList();
    }

    @Override
    public Object addToArray(Object array, List<Object> value) {
        return value;
    }

    protected Object binaryValue(byte[] value) {
        return value;
    }

    protected Object booleanValue(String value) {
        return (value != null ? (isEmpty(value) ? nullValue() : parseBoolean(value)) : nullValue());
    }

    private boolean isEmpty(String value) {
        return value.length() == 0 && emptyAsNull;
    }

    protected Object parseBoolean(String value) {
        return Boolean.parseBoolean(value);
    }

    protected Object doubleValue(String value) {
        return (value != null ? (isEmpty(value) ? nullValue() : parseDouble(value)) : nullValue());
    }

    protected Object parseDouble(String value) {
        return Double.parseDouble(value);
    }

    protected Object floatValue(String value) {
        return (value != null ? (isEmpty(value) ? nullValue() : parseFloat(value)) : nullValue());
    }

    protected Object parseFloat(String value) {
        return Float.parseFloat(value);
    }

    protected Object longValue(String value) {
        return (value != null ? (isEmpty(value) ? nullValue() : parseLong(value)) : nullValue());
    }

    protected Object parseLong(String value) {
        return Long.parseLong(value);
    }

    protected Object intValue(String value) {
        return (value != null ? (isEmpty(value) ? nullValue() : parseInteger(value)) : nullValue());
    }

    protected Object parseInteger(String value) {
        return Integer.parseInt(value);
    }

    protected Object textValue(String value) {
        return (value != null ? (!StringUtils.hasText(value) && emptyAsNull ? nullValue() : parseString(value)) : nullValue());
    }

    protected Object parseString(String value) {
        return value;
    }

    protected Object nullValue() {
        return null;
    }

    protected Object date(String value) {
        throw new UnsupportedOperationException("wip");
    }

    @Override
    public void setSettings(Settings settings) {
        emptyAsNull = settings.getFieldReadEmptyAsNull();
    }
}