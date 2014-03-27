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

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.hadoop.rest.dto.mapping.Field;
import org.elasticsearch.hadoop.serialization.Parser.NumberType;
import org.elasticsearch.hadoop.serialization.Parser.Token;
import org.elasticsearch.hadoop.serialization.builder.ValueReader;
import org.elasticsearch.hadoop.serialization.json.JacksonJsonParser;
import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.BytesArray;
import org.elasticsearch.hadoop.util.FastByteArrayInputStream;
import org.elasticsearch.hadoop.util.IOUtils;

/**
 * Class handling the conversion of data from ES to target objects. It performs tree navigation tied to a potential ES mapping (if available).
 * Expected to read a _search response.
 */
public class ScrollReader {

    private static final Log log = LogFactory.getLog(ScrollReader.class);

    private Parser parser;
    private final ValueReader reader;
    private final Map<String, FieldType> esMapping;
    private final boolean trace = log.isTraceEnabled();

    private static final String[] HITS = new String[] { "hits" };
    private static final String[] ID = new String[] { "_id" };
    private static final String[] FIELDS = new String[] { "fields" };
    private static final String[] SOURCE = new String[] { "_source" };
    private static final String[] TOTAL = new String[] { "hits", "total" };

    public ScrollReader(ValueReader reader, Field rootField) {
        this.reader = reader;
        esMapping = Field.toLookupMap(rootField);
    }


    public List<Object[]> read(InputStream content) throws IOException {
        Assert.notNull(content);

        if (log.isTraceEnabled()) {
            //copy content
            BytesArray copy = IOUtils.asBytes(content);
            content = new FastByteArrayInputStream(copy);
            log.trace("About to parse scroll content " + copy);
        }

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
        Token token = ParsingUtils.seek(parser, HITS);

        // move through the list and for each hit, extract the _id and _source
        Assert.isTrue(token == Token.START_ARRAY, "invalid response");

        List<Object[]> results = new ArrayList<Object[]>();

        for (token = parser.nextToken(); token != Token.END_ARRAY; token = parser.nextToken()) {
            results.add(readHit());
        }

        return results;
    }

    private Object[] readHit() {
        Token t = parser.currentToken();
        Assert.isTrue(t == Token.START_OBJECT, "expected object, found " + t);
        Assert.notNull(ParsingUtils.seek(parser, ID), "no id found");
        Object[] result = new Object[2];
        result[0] = parser.text();
        Token seek = ParsingUtils.seek(parser, SOURCE, FIELDS);
        // no fields found
        result[1] = (seek == null ? Collections.emptyMap() : read(t, null));

        if (trace) {
            log.trace(String.format("Read hit result [%s]=[%s]", result[0], result[1]));
        }

        return result;
    }

    private long hits() {
        ParsingUtils.seek(parser, TOTAL);
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
        // special case of handing null (as text() will return "null")
        if (parser.currentToken() == Token.VALUE_NULL) {
            return null;
        }

        Object obj = reader.readValue(parser, parser.text(), esType);
        parser.nextToken();
        return obj;
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
        // create only one element since with fields, we always get arrays which creates unneeded allocations
        List<Object> content = new ArrayList<Object>(1);
        for (; parser.currentToken() != Token.END_ARRAY;) {
            content.add(read(parser.currentToken(), fieldMapping));
        }

        // eliminate END_ARRAY
        parser.nextToken();

        array = reader.addToArray(array, content);
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

        for (; parser.currentToken() != Token.END_OBJECT; ) {
            String currentName = parser.currentName();
            String nodeMapping = fieldMapping;

            if (nodeMapping != null) {
                nodeMapping = fieldMapping + "." + currentName;
            }
            else {
                nodeMapping = currentName;
            }

            // Must point to field name
            Object fieldName = reader.readValue(parser, currentName, FieldType.STRING);
            // And then the value...
            reader.addToMap(map, fieldName, read(parser.nextToken(), nodeMapping));
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
            // nested type
            return FieldType.OBJECT;
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
            case BIG_DECIMAL:
                throw new UnsupportedOperationException();
            case BIG_INTEGER:
                throw new UnsupportedOperationException();
            default:
                break;
            }
                break;
        default:
            break;
            }
        return esType;
        }
}