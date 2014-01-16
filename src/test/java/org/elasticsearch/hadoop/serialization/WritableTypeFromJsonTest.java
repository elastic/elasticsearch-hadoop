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
import org.elasticsearch.hadoop.mr.WritableValueReader;
import org.elasticsearch.hadoop.serialization.Parser.NumberType;
import org.elasticsearch.hadoop.serialization.Parser.Token;
import org.elasticsearch.hadoop.serialization.builder.ValueReader;
import org.elasticsearch.hadoop.serialization.json.JacksonJsonParser;
import org.junit.Before;
import org.junit.Test;

public class WritableTypeFromJsonTest {

    private ValueReader vr = new WritableValueReader();

    @Before
    public void start() {
        vr = new WritableValueReader();
    }

    @Test
    public void testNull() {
        writableTypeFromJson("null");
    }

    @Test
    public void testEmptyString() {
        writableTypeFromJson("");
    }

    @Test
    public void testString() {
        writableTypeFromJson("\"someText\"");
    }

    @Test
    public void testInteger() {
        writableTypeFromJson("123");
    }

    @Test
    public void testLong() {
        writableTypeFromJson("321");
    }

    @Test
    public void testDouble() {
        writableTypeFromJson("12.3e8");
    }

    @Test
    public void testFloat() {
        writableTypeFromJson("1.3");
    }

    @Test
    public void testBoolean() {
        writableTypeFromJson("true");
    }

    @Test
    public void testByteArray() {
        writableTypeFromJson("\"" + Base64Variants.getDefaultVariant().encode("byte array".getBytes()) + "\"");
    }

    //@Test
    public void testArray() {
        writableTypeFromJson("[ \"one\" ,\"two\"]");
    }

    //@Test
    public void testMap() {
        writableTypeFromJson("{ one:1, two:2 }");
    }

    private void writableTypeFromJson(String json) {
        JacksonJsonParser parser = new JacksonJsonParser(json.getBytes());
        parser.nextToken();
        Object readValue = vr.readValue(parser, parser.text(), fromJson(parser, parser.currentToken()));
        System.out.println(readValue);
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