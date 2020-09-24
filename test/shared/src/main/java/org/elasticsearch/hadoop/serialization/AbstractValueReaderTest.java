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
package org.elasticsearch.hadoop.serialization;

import org.codehaus.jackson.Base64Variants;
import org.elasticsearch.hadoop.serialization.Parser.NumberType;
import org.elasticsearch.hadoop.serialization.Parser.Token;
import org.elasticsearch.hadoop.serialization.builder.ValueReader;
import org.elasticsearch.hadoop.serialization.json.JacksonJsonParser;
import org.junit.Before;
import org.junit.Test;

public abstract class AbstractValueReaderTest {

    public ValueReader vr;

    public abstract ValueReader createValueReader();
    public abstract void checkNull(Object typeFromJson);
    public abstract void checkEmptyString(Object typeFromJson);
    public abstract void checkString(Object typeFromJson);
    public abstract void checkInteger(Object typeFromJson);
    public abstract void checkLong(Object typeFromJson);
    public abstract void checkDouble(Object typeFromJson);
    public abstract void checkFloat(Object typeFromJson);
    public abstract void checkBoolean(Object typeFromJson);
    public abstract void checkByteArray(Object typeFromJson, String encode);
    public abstract void checkBinary(Object typeFromJson, byte[] encode);


    @Before
    public void start() {
        vr = createValueReader();
    }

    @Test
    public void testNull() {
        checkNull(typeFromJson("null"));
    }

    @Test
    public void testEmptyString() {
        checkEmptyString(typeFromJson(""));
    }

    @Test
    public void testString() {
        checkString(typeFromJson("\"someText\""));
    }

    @Test
    public void testInteger() {
        checkInteger(typeFromJson("" + Integer.MAX_VALUE));
    }

    @Test
    public void testLong() {
        checkLong(typeFromJson("" + Long.MAX_VALUE));
    }

    @Test
    public void testDouble() {
        checkDouble(typeFromJson("" + Double.MAX_VALUE));
    }

    @Test
    public void testFloat() {
        checkFloat(typeFromJson("" + Float.MAX_VALUE));
    }

    @Test
    public void testBoolean() {
        checkBoolean(typeFromJson("true"));
    }

    @Test
    public void testByteArray() {
        String encode = Base64Variants.getDefaultVariant().encode("byte array".getBytes());
        checkByteArray(typeFromJson("\"" + encode + "\""), encode);
    }

    @Test
    public void testBinary() {
        String encode = Base64Variants.getDefaultVariant().encode("binary blob".getBytes());
        checkBinary(readFromJson("\"" + encode + "\"", FieldType.BINARY), encode.getBytes());
    }

    //@Test
    public void testArray() {
        typeFromJson("[ \"one\" ,\"two\"]");
    }

    //@Test
    public void testMap() {
        typeFromJson("{ one:1, two:2 }");
    }

    private JacksonJsonParser parserFromJson(String json) {
        JacksonJsonParser parser = new JacksonJsonParser(json.getBytes());
        parser.nextToken();
        return parser;
    }

    private Object readFromJson(String json, FieldType esType) {
        JacksonJsonParser parser = parserFromJson(json);
        return vr.readValue(parser, parser.text(), esType);
    }

    private Object typeFromJson(String json) {
        JacksonJsonParser parser = parserFromJson(json);
        return vr.readValue(parser, parser.text(), fromJson(parser, parser.currentToken()));
    }

    private static FieldType fromJson(Parser parser, Token currentToken) {
        if (currentToken == null) {
            return null;
        }
        switch (currentToken) {
        case VALUE_NULL:
            return FieldType.NULL;
        case VALUE_BOOLEAN:
            return FieldType.BOOLEAN;
        case VALUE_STRING:
            return FieldType.STRING;
        case VALUE_NUMBER:
            NumberType numberType = parser.numberType();
            switch (numberType) {
            case INT:
                return FieldType.INTEGER;
            case LONG:
                return FieldType.LONG;
            case FLOAT:
                return FieldType.FLOAT;
            case DOUBLE:
                return FieldType.DOUBLE;
            }
        }
        return null;
    }
}