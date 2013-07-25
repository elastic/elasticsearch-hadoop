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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.hadoop.rest.dto.mapping.Field;
import org.elasticsearch.hadoop.serialization.Parser.NumberType;
import org.elasticsearch.hadoop.serialization.Parser.Token;
import org.elasticsearch.hadoop.serialization.json.JacksonJsonParser;
import org.elasticsearch.hadoop.util.Assert;

/**
 * Class handling the conversion of data from ES to target objects. It performs tree navigation tied to a potential ES mapping (if available).
 * Expected to read a _search response.
 */
public class ScrollReader {

    private Parser parser;
    private final FieldReader reader;
    private final Map<String, FieldType> esMapping;

    public ScrollReader(FieldReader reader, Field rootField) {
        this.reader = reader;

        Map<String, FieldType> flds = null;

        // expand fields into a lookup table
        if (rootField != null) {
            flds = new LinkedHashMap<String, FieldType>();
            ParsingUtils.add(flds, rootField, null);
        }
        esMapping = (flds != null ? flds : Collections.<String, FieldType> emptyMap());
    }


    public List<Object[]> read(byte[] content) {
        Assert.notNull(content);
        this.parser = new JacksonJsonParser(content);

        try {
            return read();
        } finally {
            parser.close();
        }
    }

    private List<Object[]> read() {
        // check hits/total
        if (hits() == 0) {
            return null;
        }

        // move to hits/hits
        Token token = ParsingUtils.seek("hits", parser);

        // move through the list and for each hit, extract the _id and _source
        Assert.isTrue(token == Token.START_ARRAY, "invalid response");

        List<Object[]> results = new ArrayList<Object[]>();

        for (parser.nextToken(); token != Token.END_ARRAY; token = parser.nextToken()) {
            results.add(readHit());
        }

        return results;
    }

    private Object[] readHit() {
        Token t = parser.currentToken();
        Object[] result = new Object[2];
        Assert.isTrue(t == Token.START_OBJECT, "expected object");
        Assert.notNull(ParsingUtils.seek("_id", parser), "no id found");
        result[0] = parser.text();
        Assert.notNull(ParsingUtils.seek("_source", parser), "no _source found");
        result[1] = read(t, null);
        return result;
    }

    private long hits() {
        ParsingUtils.seek("hits/total", parser);
        long hits = parser.longValue();
        return hits;
    }


    protected Object read(Token t, String fieldMapping) {
        // handle nested nodes first
        if (t == Token.START_OBJECT) {
            return map(fieldMapping);
        }
        else if (t == Token.START_ARRAY) {
            return list(fieldMapping);
        }

        FieldType esType = mapping(fieldMapping);

        if (t.isValue()) {
            return parseValue(esType);
        }
        return null;
    }

    private Object parseValue(FieldType esType) {
        return reader.readValue(parser, parser.text(), esType);
    }

    protected Object list(String fieldMapping) {
        Token t = parser.currentToken();

        if (t == null) {
            t = parser.nextToken();
        }
        if (t == Token.START_ARRAY) {
            t = parser.nextToken();
        }

        Object array = reader.createArray(mapping(fieldMapping));
        List<Object> content = new ArrayList<Object>();
        for (; t != Token.END_ARRAY; t = parser.nextToken()) {
            content.add(read(t, fieldMapping));
        }

        // eliminate END_ARRAY
        parser.nextToken();

        reader.addToArray(array, content);
        return array;
    }

    protected Object map(String fieldMapping) {
        Token t = parser.currentToken();

        if (t == null) {
            t = parser.nextToken();
        }
        if (t == Token.START_OBJECT) {
            t = parser.nextToken();
        }

        Object map = reader.createMap();

        for (; t != Token.END_OBJECT; t = parser.nextToken()) {
            String currentName = parser.currentName();
            String nodeMapping = fieldMapping;

            if (nodeMapping != null) {
                nodeMapping += fieldMapping + "/" + currentName;
            }
            else {
                nodeMapping = currentName;
            }

            // Must point to field name
            Object fieldName = reader.readValue(parser, currentName, FieldType.STRING);
            // And then the value...
            t = parser.nextToken();
            reader.addToMap(map, fieldName, read(t, nodeMapping));
        }

        // eliminate END_OBJECT
        parser.nextToken();

        return map;
    }

    private FieldType mapping(String fieldMapping) {
        FieldType esType = esMapping.get(fieldMapping);

        if (esType != null) {
            return esType;
        }

        // fall back to JSON
        Token currentToken = parser.currentToken();
        if (!currentToken.isValue()) {
            Assert.notNull(esType, "Expected a value but got " + currentToken);
        }

        switch (currentToken) {
        case VALUE_NULL:
            esType = FieldType.NULL;
                break;
        case VALUE_BOOLEAN:
            esType = FieldType.BOOLEAN;
                break;
        case VALUE_STRING:
            esType = FieldType.STRING;
                break;
        case VALUE_NUMBER:
            NumberType numberType = parser.numberType();
            switch (numberType) {
            case INT:
                esType = FieldType.INTEGER;
                break;
            case LONG:
                esType = FieldType.LONG;
                break;
            case FLOAT:
                esType = FieldType.FLOAT;
                break;
            case DOUBLE:
                esType = FieldType.DOUBLE;
                break;
            }
                break;
            }
        return esType;
        }
}