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

import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.hadoop.cfg.PropertiesSettings;
import org.elasticsearch.hadoop.serialization.dto.mapping.FieldParser;
import org.elasticsearch.hadoop.serialization.dto.mapping.Mapping;
import org.elasticsearch.hadoop.util.BytesArray;
import org.elasticsearch.hadoop.util.FastByteArrayInputStream;
import org.elasticsearch.hadoop.util.FastByteArrayOutputStream;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class PartitionDefinitionTest {
    @Test
    public void testWritable() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonParser jsonParser = mapper.getJsonFactory()
                .createJsonParser(getClass().getResourceAsStream("/org/elasticsearch/hadoop/serialization/dto/mapping/basic.json"));
        Map<String, Object> map =
                (Map<String, Object>) mapper.readValue(jsonParser, Map.class);
        Mapping mapping = FieldParser.parseMapping(map).getResolvedView();
        PropertiesSettings settings = new PropertiesSettings();
        settings.setProperty("setting1", "value1");
        settings.setProperty("setting2", "value2");
        PartitionDefinition expected = new PartitionDefinition(settings, mapping, "foo", 12,
                new String[] {"localhost:9200", "otherhost:9200"});
        BytesArray bytes = writeWritablePartition(expected);
        PartitionDefinition def = readWritablePartition(bytes);
        assertPartitionEquals(expected, def);
    }

    @Test
    public void testSerializable() throws IOException, ClassNotFoundException {
        ObjectMapper mapper = new ObjectMapper();
        JsonParser jsonParser = mapper.getJsonFactory()
                .createJsonParser(getClass().getResourceAsStream("/org/elasticsearch/hadoop/serialization/dto/mapping/basic.json"));
        Map<String, Object> map =
                (Map<String, Object>) mapper.readValue(jsonParser, Map.class);
        Mapping mapping = FieldParser.parseMapping(map).getResolvedView();
        PropertiesSettings settings = new PropertiesSettings();
        settings.setProperty("setting1", "value1");
        settings.setProperty("setting2", "value2");
        PartitionDefinition expected = new PartitionDefinition(settings, mapping, "bar", 37,
                new String[] {"localhost:9200", "otherhost:9200"});
        BytesArray bytes = writeSerializablePartition(expected);
        PartitionDefinition def = readSerializablePartition(bytes);
        assertPartitionEquals(expected, def);
    }

    @Test
    public void testWritableWithSlice() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonParser jsonParser = mapper.getJsonFactory()
                .createJsonParser(getClass().getResourceAsStream("/org/elasticsearch/hadoop/serialization/dto/mapping/basic.json"));
        Map<String, Object> map =
                (Map<String, Object>) mapper.readValue(jsonParser, Map.class);
        Mapping mapping = FieldParser.parseMapping(map).getResolvedView();
        PropertiesSettings settings = new PropertiesSettings();
        settings.setProperty("setting1", "value1");
        settings.setProperty("setting2", "value2");
        PartitionDefinition expected = new PartitionDefinition(settings, mapping, "foo", 12, new PartitionDefinition.Slice(10, 27),
                new String[] {"localhost:9200", "otherhost:9200"});
        BytesArray bytes = writeWritablePartition(expected);
        PartitionDefinition def = readWritablePartition(bytes);
        assertPartitionEquals(expected, def);
    }

    @Test
    public void testSerializableWithSlice() throws IOException, ClassNotFoundException {
        ObjectMapper mapper = new ObjectMapper();
        JsonParser jsonParser = mapper.getJsonFactory()
                .createJsonParser(getClass().getResourceAsStream("/org/elasticsearch/hadoop/serialization/dto/mapping/basic.json"));
        Map<String, Object> map =
                (Map<String, Object>) mapper.readValue(jsonParser, Map.class);
        Mapping mapping = FieldParser.parseMapping(map).getResolvedView();
        PropertiesSettings settings = new PropertiesSettings();
        settings.setProperty("setting1", "value1");
        settings.setProperty("setting2", "value2");

        PartitionDefinition expected = new PartitionDefinition(settings, mapping, "bar", 37,
                new PartitionDefinition.Slice(13, 35),  new String[] {"localhost:9200", "otherhost:9200"});
        BytesArray bytes = writeSerializablePartition(expected);
        PartitionDefinition def = readSerializablePartition(bytes);
        assertPartitionEquals(expected, def);
    }

    static PartitionDefinition readSerializablePartition(BytesArray bytes) throws IOException, ClassNotFoundException {
        FastByteArrayInputStream in = new FastByteArrayInputStream(bytes);
        ObjectInputStream ois = new ObjectInputStream(in);
        return (PartitionDefinition) ois.readObject();
    }

    static BytesArray writeSerializablePartition(PartitionDefinition def) throws IOException {
        FastByteArrayOutputStream out = new FastByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(out);
        try {
            oos.writeObject(def);
            oos.flush();
            return out.bytes();
        } finally {
            oos.close();
        }
    }

    static PartitionDefinition readWritablePartition(BytesArray bytes) throws IOException {
        FastByteArrayInputStream in = new FastByteArrayInputStream(bytes);
        try {
            DataInputStream di = new DataInputStream(in);
            return new PartitionDefinition(di);
        } finally {
            in.close();
        }
    }

    static BytesArray writeWritablePartition(PartitionDefinition def) throws IOException {
        FastByteArrayOutputStream out = new FastByteArrayOutputStream();
        DataOutputStream da = new DataOutputStream(out);
        try {
            def.write(da);
            da.flush();
            return out.bytes();
        } finally {
            da.close();
        }
    }

    static void assertPartitionEquals(PartitionDefinition p1, PartitionDefinition p2) {
        assertEquals(p1, p2);
        assertArrayEquals(p1.getLocations(), p2.getLocations());
        // the settings, the mapping and the locations are ignored in PartitionDefinition#equals
        // we need to test them separately
        assertEquals(p1.getSerializedSettings(), p2.getSerializedSettings());
        assertEquals(p1.getSerializedMapping(), p2.getSerializedMapping());
    }
}
