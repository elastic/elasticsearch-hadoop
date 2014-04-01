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

import java.io.InputStream;

import org.elasticsearch.hadoop.serialization.Parser.Token;
import org.elasticsearch.hadoop.serialization.json.JacksonJsonParser;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class JsonPathTest {

    private Parser parser;

    @Before
    public void before() {
        InputStream in = getClass().getResourceAsStream("parser-test.json");
        parser = new JacksonJsonParser(in);
    }

    @After
    public void after() {
        parser.close();
    }

    @Test
    public void testNoPath() throws Exception {
        assertNull(ParsingUtils.seek(null, parser));
        assertNull(parser.currentToken());
        assertNull(ParsingUtils.seek("", parser));
        assertNull(parser.currentToken());
        assertNull(ParsingUtils.seek(" ", parser));
        assertNull(parser.currentToken());
    }

    @Test
    public void testNonExistingToken() throws Exception {
        assertNull(ParsingUtils.seek("nosuchtoken", parser));
        assertNull(parser.nextToken());
    }

    @Test
    public void testFieldName() throws Exception {
        assertNotNull(ParsingUtils.seek("age", parser));
        assertEquals(Token.VALUE_NUMBER, parser.currentToken());
        assertEquals("age", parser.currentName());
    }

    @Test
    public void testOneLevelNestedField() throws Exception {
        assertNotNull(ParsingUtils.seek("address.state", parser));
        assertEquals(Token.VALUE_STRING, parser.currentToken());
        assertEquals("state", parser.currentName());
    }
    @Test
    public void testFieldNestedButNotOnFirstLevel() throws Exception {
        assertNull(ParsingUtils.seek("state", parser));
        assertNull(parser.nextToken());
        assertNull(parser.currentToken());
    }
}