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

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.hive.HiveFieldExtractor;
import org.elasticsearch.hadoop.serialization.HiveTypeToJsonTest.MyHiveType;
import org.elasticsearch.hadoop.serialization.field.ConstantFieldExtractor;
import org.elasticsearch.hadoop.serialization.field.FieldExtractor;
import org.elasticsearch.hadoop.util.TestSettings;
import org.junit.Test;

import static org.junit.Assert.*;

import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.*;

public class HiveFieldExtractorTests {

    private Object extract(String field, Object target) {
        TestSettings cfg = new TestSettings();
        cfg.setProperty(ConstantFieldExtractor.PROPERTY, field);

        ConstantFieldExtractor extractor = new HiveFieldExtractor() {

            @Override
            public void processField(Settings settings, List<String> fl) {
                fieldNames = fl;
            }
        };

        extractor.setSettings(cfg);
        return extractor.field(target);
    }

    @Test
    public void testHiveFieldExtractorNestedNotFound() throws Exception {
        Map<String, String> m = new LinkedHashMap<String, String>();
        assertEquals(FieldExtractor.NOT_FOUND, extract("key", m));
    }

    @Test(expected = EsHadoopIllegalArgumentException.class)
    public void testHiveFieldExtractorNested() throws Exception {
        List<String> nested = Arrays.asList(new String[] { "bar", "bor" });
        List<TypeInfo> types = Arrays.asList(new TypeInfo[] { stringTypeInfo, intTypeInfo });
        MyHiveType struct = new MyHiveType(Arrays.asList(new Object[] { new Text("found"), new IntWritable(2) }), getStructTypeInfo(nested, types));

        List<String> topNames = Arrays.asList(new String[] { "foo", "far" });
        List<TypeInfo> topTypes = Arrays.asList(new TypeInfo[] { getStructTypeInfo(nested, types), intTypeInfo });
        MyHiveType topStruct = new MyHiveType(Arrays.asList(new Object[] { struct, new IntWritable(1) }), getStructTypeInfo(topNames, topTypes));

        String colDesc = "bar,bor";
        assertEquals(new Text("found"), extract("foo.bar", topStruct));
    }

    @Test
    public void testHiveFieldExtractorTopLevel() throws Exception {
        List<String> names = Arrays.asList(new String[] { "one", "two" });
        List<TypeInfo> types = Arrays.asList(new TypeInfo[] { stringTypeInfo, intTypeInfo });
        MyHiveType struct = new MyHiveType(Arrays.asList(new Object[] { new Text("first"), new IntWritable(2) }), getStructTypeInfo(names, types));

        String colDesc = "one,two";
        assertEquals("first", extract("one", struct));
    }
}