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
package org.elasticsearch.hadoop.util;

import org.junit.Test;

import static org.junit.Assert.*;

public class StringUtilsTest {

    @Test
    public void testJsonEncodingQuote() {
        String value = "foo\"bar";
        String jsonEscaped = "foo\\\"bar";
        assertEquals(jsonEscaped, StringUtils.jsonEncoding(value));
    }

    @Test
    public void testParseIpInEs1x() {
        assertEquals("1.2.3.4", StringUtils.parseIpAddress("inet[/1.2.3.4:9200]").ip);
    }

    @Test
    public void testParseIpInEs1xWithHostName() {
        assertEquals("11.22.33.44", StringUtils.parseIpAddress("inet[foobar/11.22.33.44:9200]").ip);
    }

    @Test
    public void testParseIpInEs2x() {
        assertEquals("111.222.333.444", StringUtils.parseIpAddress("111.222.333.444:9200").ip);
    }

    @Test
    public void testParseIpInEs2xWithHostName() {
        assertEquals("11.222.3.4", StringUtils.parseIpAddress("foobar/11.222.3.4:9200").ip);
    }

}
