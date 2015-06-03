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
import java.util.List;

import org.elasticsearch.hadoop.serialization.json.JacksonJsonParser;
import org.elasticsearch.hadoop.util.FastByteArrayInputStream;
import org.elasticsearch.hadoop.util.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;

import static org.hamcrest.CoreMatchers.containsString;

import static org.hamcrest.Matchers.is;

public class JsonValuePathTest {

    private Parser parser;

    @Before
    public void before() throws Exception {
        InputStream in = new FastByteArrayInputStream(IOUtils.asBytes(getClass().getResourceAsStream("parser-test-nested.json")));
        parser = new JacksonJsonParser(in);
    }

    @After
    public void after() {
        parser.close();
    }

    @Test
    public void testFirstLevel() throws Exception {
        List<Object> vals = ParsingUtils.values(parser, "firstName", "foo", "age");
        assertEquals(3, vals.size());
        assertEquals("John", vals.get(0));
        assertSame(ParsingUtils.NOT_FOUND, vals.get(1));
        assertEquals(25, vals.get(2));
    }

    @Test
    public void testSecondLevel() throws Exception {
        List<Object> vals = ParsingUtils.values(parser, "address.state", "address.foo", "address.building.floors", "address.building.bar");
        assertEquals(4, vals.size());
        assertEquals("NY", vals.get(0));
        assertSame(ParsingUtils.NOT_FOUND, vals.get(1));
        assertEquals(10, vals.get(2));
        assertSame(ParsingUtils.NOT_FOUND, vals.get(3));
    }

    @Test
    public void testRichObject() throws Exception {
        List<Object> vals = ParsingUtils.values(parser, "address");
        assertEquals(1, vals.size());
        assertThat(vals.get(0).toString(), containsString("floors"));
    }

    @Test
    public void testMultipleNestedMatches1() throws Exception {
        List<Object> vals = ParsingUtils.values(parser, "firstName");
        assertEquals(1, vals.size());
        assertThat(vals.get(0).toString(), containsString("John"));
    }

    @Test
    public void testMultipleNestedMatches2() throws Exception {
        List<Object> vals = ParsingUtils.values(parser, "age");
        assertEquals(1, vals.size());
        assertThat((Integer) vals.get(0), is(Integer.valueOf(25)));
    }

    @Test
    public void testRichObjectNested() throws Exception {
        List<Object> vals = ParsingUtils.values(parser, "address.building");
        assertEquals(1, vals.size());
        assertThat(vals.get(0).toString(), containsString("floors"));
    }

    @Test
    public void testCorrectLevelMatched() throws Exception {
        List<Object> vals = ParsingUtils.values(parser, "state");
        assertEquals(1, vals.size());
        assertThat(vals.get(0).toString(), containsString("CA"));
    }

    @Test
    public void testSmallerMixedLevels() throws Exception {
        List<Object> vals = ParsingUtils.values(parser, "firstName", "address.state", "state");
        assertEquals(3, vals.size());
        assertEquals("John", vals.get(0));
        assertEquals("NY", vals.get(1));
        assertEquals("CA", vals.get(2));
    }

    @Test
    public void testMixedLevels() throws Exception {
        List<Object> vals = ParsingUtils.values(parser, "firstName", "address.building.floors", "address.decor.walls", "zzz");
        assertEquals(4, vals.size());
        assertEquals("John", vals.get(0));
        assertEquals(10, vals.get(1));
        assertEquals("white", vals.get(2));
        assertEquals("end", vals.get(3));
    }
}