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
import org.elasticsearch.hadoop.serialization.dto.mapping.Field;
import org.elasticsearch.hadoop.util.FastByteArrayInputStream;
import org.elasticsearch.hadoop.util.FastByteArrayOutputStream;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class PartitionDefinitionTest {
    @Test
    public void testWritable() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonParser jsonParser = mapper.getJsonFactory()
                .createJsonParser(getClass().getResourceAsStream("/org/elasticsearch/hadoop/serialization/dto/mapping/basic.json"));
        Map<String, Object> map =
                (Map<String, Object>) mapper.readValue(jsonParser, Map.class);
        Field mapping = Field.parseField(map);
        PropertiesSettings settings = new PropertiesSettings();
        settings.setProperty("setting1", "value1");
        settings.setProperty("setting2", "value2");
        PartitionDefinition expected = new PartitionDefinition("foo", 12, new PartitionDefinition.Slice(10, 27), settings, mapping);
        FastByteArrayOutputStream out = new FastByteArrayOutputStream();
        DataOutputStream da = new DataOutputStream(out);
        expected.write(da);
        out.close();

        FastByteArrayInputStream in = new FastByteArrayInputStream(out.bytes());
        DataInputStream di = new DataInputStream(in);
        PartitionDefinition def = new PartitionDefinition(di);
        assertEquals(def, expected);
        // the settings and the mapping are ignored in PartitionDefinition#equals
        // we need to test them separately
        assertEquals(def.getSerializedSettings(), expected.getSerializedSettings());
        assertEquals(def.getSerializedMapping(), expected.getSerializedMapping());

    }

    @Test
    public void testSerializable() throws IOException, ClassNotFoundException {
        ObjectMapper mapper = new ObjectMapper();
        JsonParser jsonParser = mapper.getJsonFactory()
                .createJsonParser(getClass().getResourceAsStream("/org/elasticsearch/hadoop/serialization/dto/mapping/basic.json"));
        Map<String, Object> map =
                (Map<String, Object>) mapper.readValue(jsonParser, Map.class);
        Field mapping = Field.parseField(map);
        PropertiesSettings settings = new PropertiesSettings();
        settings.setProperty("setting1", "value1");
        settings.setProperty("setting2", "value2");
        PartitionDefinition expected = new PartitionDefinition("bar", 37, new PartitionDefinition.Slice(13, 35), settings, mapping);
        FastByteArrayOutputStream out = new FastByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(out);
        oos.writeObject(expected);
        oos.close();

        FastByteArrayInputStream in = new FastByteArrayInputStream(out.bytes());
        ObjectInputStream ois = new ObjectInputStream(in);
        PartitionDefinition def = (PartitionDefinition) ois.readObject();
        assertEquals(def, expected);
        // the settings and the mapping are ignored in PartitionDefinition#equals
        // we need to test them separately
        assertEquals(def.getSerializedSettings(), expected.getSerializedSettings());
        assertEquals(def.getSerializedMapping(), expected.getSerializedMapping());
    }
}
