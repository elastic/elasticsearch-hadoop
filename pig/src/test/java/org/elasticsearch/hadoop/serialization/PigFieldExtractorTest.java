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

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.pig.ResourceSchema;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.util.Utils;
import org.elasticsearch.hadoop.pig.PigFieldExtractor;
import org.elasticsearch.hadoop.pig.PigTuple;
import org.elasticsearch.hadoop.serialization.field.ConstantFieldExtractor;
import org.elasticsearch.hadoop.serialization.field.FieldExtractor;
import org.elasticsearch.hadoop.util.TestSettings;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PigFieldExtractorTest {

    private Object extract(String field, Object target) {
        TestSettings cfg = new TestSettings();
        cfg.setProperty(ConstantFieldExtractor.PROPERTY, field);

        ConstantFieldExtractor cfe = new PigFieldExtractor();
        cfe.setSettings(cfg);
        return cfe.field(target);
    }

    @Test
    public void testMapFieldExtractorTopLevel() throws Exception {
        PigTuple tuple = createTuple("some string", createSchema("name:chararray"));
        assertEquals("some string", extract("name", tuple));
    }

    @Test
    public void testMapFieldExtractorNestedNotFound() throws Exception {
        Map<String, String> m = new LinkedHashMap<String, String>();
        assertEquals(FieldExtractor.NOT_FOUND, extract("key", m));
    }

    @Test
    public void testMapWritableFieldExtractorNested() throws Exception {
        PigTuple nested = createTuple("found", createSchema("bar:chararray"));
        PigTuple top = createTuple(nested, createSchema("foo: (bar: chararray)"));
        assertEquals("found", extract("foo.bar", top));
    }

    private ResourceSchema createSchema(String schema) {
        try {
            return new ResourceSchema(Utils.getSchemaFromString(schema));
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private PigTuple createTuple(Object obj, ResourceSchema schema) {
        PigTuple tuple = new PigTuple(schema);
        tuple.setTuple(TupleFactory.getInstance().newTuple(obj));
        return tuple;
    }
}
