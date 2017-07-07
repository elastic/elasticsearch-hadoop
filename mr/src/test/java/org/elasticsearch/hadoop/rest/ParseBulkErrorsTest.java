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
package org.elasticsearch.hadoop.rest;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;

import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.hadoop.serialization.ParsingUtils;
import org.elasticsearch.hadoop.serialization.json.BackportedObjectReader;
import org.elasticsearch.hadoop.serialization.json.JacksonJsonParser;
import org.elasticsearch.hadoop.util.BytesArray;
import org.elasticsearch.hadoop.util.IOUtils;
import org.elasticsearch.hadoop.util.TestSettings;
import org.elasticsearch.hadoop.util.TrackingBytesArray;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ParseBulkErrorsTest {

    private RestClient rc;

    @Before
    public void before() {
        rc = new RestClient(new TestSettings());
    }

    @After
    public void after() {
        rc.close();
    }

    @Test
    public void testParseItems() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        InputStream in = getClass().getResourceAsStream("/org/elasticsearch/hadoop/rest/bulk-error.json");
        JsonParser parser = mapper.getJsonFactory().createJsonParser(in);
        assertNotNull(ParsingUtils.seek(new JacksonJsonParser(parser), "items"));

        BackportedObjectReader r = BackportedObjectReader.create(mapper, Map.class);

        for (Iterator<Map> iterator = r.readValues(parser); iterator.hasNext();) {
            Map map = mapper.readValue(parser, Map.class);
            String error = (String) ((Map) map.values().iterator().next()).get("error");
            assertNotNull(error);
            assertTrue(error.contains("document already exists"));
        }
    }

    @Test
    public void testParseBulkErrorsInES2x() throws Exception {
        String inputEntry = IOUtils.asString(getClass().getResourceAsStream("bulk-retry-input-template.json"));

        TrackingBytesArray inputData = new TrackingBytesArray(new BytesArray(128));

        inputData.copyFrom(new BytesArray(inputEntry.replace("w", "A")));
        inputData.copyFrom(new BytesArray(inputEntry.replace("w", "B")));
        inputData.copyFrom(new BytesArray(inputEntry.replace("w", "C")));
        inputData.copyFrom(new BytesArray(inputEntry.replace("w", "D")));
        inputData.copyFrom(new BytesArray(inputEntry.replace("w", "E")));

        assertEquals(5, inputData.entries());
        assertEquals("{0, 1, 2, 3, 4}", inputData.leftoversPosition().toString());

        Response response = new SimpleResponse(HttpStatus.OK, getClass().getResourceAsStream("bulk-retry-output-es2x.json"), "");
        assertTrue(rc.processBulkResponse(response, inputData).getHttpStatus() == HttpStatus.SERVICE_UNAVAILABLE);
        assertEquals(3, inputData.entries());
        assertEquals("{1, 3, 4}", inputData.leftoversPosition().toString());
        String string = inputData.toString();
        assertTrue(string.contains("B"));
        assertTrue(string.contains("D"));
        assertTrue(string.contains("E"));
    }

    @Test
    public void testParseBulkErrorsInES1x() throws Exception {
        String inputEntry = IOUtils.asString(getClass().getResourceAsStream("bulk-retry-input-template.json"));

        TrackingBytesArray inputData = new TrackingBytesArray(new BytesArray(128));

        inputData.copyFrom(new BytesArray(inputEntry.replace("w", "A")));
        inputData.copyFrom(new BytesArray(inputEntry.replace("w", "B")));
        inputData.copyFrom(new BytesArray(inputEntry.replace("w", "C")));
        inputData.copyFrom(new BytesArray(inputEntry.replace("w", "D")));
        inputData.copyFrom(new BytesArray(inputEntry.replace("w", "E")));

        assertEquals(5, inputData.entries());
        assertEquals("{0, 1, 2, 3, 4}", inputData.leftoversPosition().toString());

        Response response = new SimpleResponse(HttpStatus.OK, getClass().getResourceAsStream("bulk-retry-output-es1x.json"), "");
        assertTrue(rc.processBulkResponse(response, inputData).getHttpStatus() == HttpStatus.SERVICE_UNAVAILABLE);
        assertEquals(3, inputData.entries());
        assertEquals("{1, 3, 4}", inputData.leftoversPosition().toString());
        String string = inputData.toString();
        assertTrue(string.contains("B"));
        assertTrue(string.contains("D"));
        assertTrue(string.contains("E"));
    }

    @Test
    public void testParseBulkErrorsInES10x() throws Exception {
        String inputEntry = IOUtils.asString(getClass().getResourceAsStream("bulk-retry-input-template.json"));

        TrackingBytesArray inputData = new TrackingBytesArray(new BytesArray(128));

        inputData.copyFrom(new BytesArray(inputEntry.replace("w", "A")));
        inputData.copyFrom(new BytesArray(inputEntry.replace("w", "B")));
        inputData.copyFrom(new BytesArray(inputEntry.replace("w", "C")));
        inputData.copyFrom(new BytesArray(inputEntry.replace("w", "D")));
        inputData.copyFrom(new BytesArray(inputEntry.replace("w", "E")));

        assertEquals(5, inputData.entries());
        assertEquals("{0, 1, 2, 3, 4}", inputData.leftoversPosition().toString());

        Response response = new SimpleResponse(HttpStatus.OK, getClass().getResourceAsStream("bulk-retry-output-es10x.json"), "");
        assertTrue(rc.processBulkResponse(response, inputData).getHttpStatus() == HttpStatus.SERVICE_UNAVAILABLE);
        assertEquals(3, inputData.entries());
        assertEquals("{1, 3, 4}", inputData.leftoversPosition().toString());
        String string = inputData.toString();
        assertTrue(string.contains("B"));
        assertTrue(string.contains("D"));
        assertTrue(string.contains("E"));
    }

    @Test
    public void testParseBulkErrorsInES090x() throws Exception {
        String inputEntry = IOUtils.asString(getClass().getResourceAsStream("bulk-retry-input-template.json"));

        TrackingBytesArray inputData = new TrackingBytesArray(new BytesArray(128));

        inputData.copyFrom(new BytesArray(inputEntry.replace("w", "A")));
        inputData.copyFrom(new BytesArray(inputEntry.replace("w", "B")));
        inputData.copyFrom(new BytesArray(inputEntry.replace("w", "C")));
        inputData.copyFrom(new BytesArray(inputEntry.replace("w", "D")));
        inputData.copyFrom(new BytesArray(inputEntry.replace("w", "E")));

        assertEquals(5, inputData.entries());
        assertEquals("{0, 1, 2, 3, 4}", inputData.leftoversPosition().toString());

        Response response = new SimpleResponse(HttpStatus.OK, getClass().getResourceAsStream("bulk-retry-output-es090x.json"), "");
        assertTrue(rc.processBulkResponse(response, inputData).getHttpStatus() == HttpStatus.SERVICE_UNAVAILABLE);
        assertEquals(3, inputData.entries());
        assertEquals("{1, 3, 4}", inputData.leftoversPosition().toString());
        String string = inputData.toString();
        assertTrue(string.contains("B"));
        assertTrue(string.contains("D"));
        assertTrue(string.contains("E"));
    }

    @Test
    public void testParseBulkErrorsInES5x() throws Exception {
        String inputEntry = IOUtils.asString(getClass().getResourceAsStream("bulk-retry-input-template.json"));

        TrackingBytesArray inputData = new TrackingBytesArray(new BytesArray(128));

        inputData.copyFrom(new BytesArray(inputEntry.replace("w", "A")));
        inputData.copyFrom(new BytesArray(inputEntry.replace("w", "B")));
        inputData.copyFrom(new BytesArray(inputEntry.replace("w", "C")));
        inputData.copyFrom(new BytesArray(inputEntry.replace("w", "D")));
        inputData.copyFrom(new BytesArray(inputEntry.replace("w", "E")));

        assertEquals(5, inputData.entries());
        assertEquals("{0, 1, 2, 3, 4}", inputData.leftoversPosition().toString());

        Response response = new SimpleResponse(HttpStatus.OK, getClass().getResourceAsStream("bulk-retry-output-es5x.json"), "");
        assertTrue(rc.processBulkResponse(response, inputData).getHttpStatus() == HttpStatus.SERVICE_UNAVAILABLE);
        assertEquals(3, inputData.entries());
        assertEquals("{1, 3, 4}", inputData.leftoversPosition().toString());
        String string = inputData.toString();
        assertTrue(string.contains("B"));
        assertTrue(string.contains("D"));
        assertTrue(string.contains("E"));

    }

}