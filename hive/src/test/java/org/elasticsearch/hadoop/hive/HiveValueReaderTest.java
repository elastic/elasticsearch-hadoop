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
package org.elasticsearch.hadoop.hive;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.hadoop.serialization.ScrollReader;
import org.elasticsearch.hadoop.serialization.ScrollReader.ScrollReaderConfig;
import org.elasticsearch.hadoop.serialization.dto.mapping.Field;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class HiveValueReaderTest {

    @Test
    public void testDateMapping() throws Exception {
        ScrollReader reader = new ScrollReader(new ScrollReaderConfig(new HiveValueReader(), mapping("hive-date.json"), false, "_mapping", false, false));
        InputStream stream = getClass().getResourceAsStream("hive-date-source.json");
        List<Object[]> read = reader.read(stream).getHits();
        assertEquals(1, read.size());
        Object[] doc = read.get(0);
        Map map = (Map) doc[1];
        assertTrue(map.containsKey(new Text("type")));
        assertTrue(map.containsKey(new Text("&t")));
        assertTrue(map.get(new Text("&t")).toString().contains("2014-08-05"));
    }

    private Field mapping(String resource) throws Exception {
        InputStream stream = getClass().getResourceAsStream(resource);
        return Field.parseField(new ObjectMapper().readValue(stream, Map.class));
    }
}
