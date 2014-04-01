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
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.rest.InitializationUtils;
import org.elasticsearch.hadoop.serialization.builder.JdkValueWriter;
import org.elasticsearch.hadoop.serialization.command.BulkCommands;
import org.elasticsearch.hadoop.serialization.command.Command;
import org.elasticsearch.hadoop.util.BytesArray;
import org.elasticsearch.hadoop.util.TestSettings;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static org.junit.Assert.*;

import static org.junit.Assume.*;

@RunWith(Parameterized.class)
public class CommandTest {

    private BytesArray ba = new BytesArray(1024);
    private Map map = new LinkedHashMap();
    private String operation;
    private boolean noId = false;

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                { ConfigurationOptions.ES_OPERATION_INDEX },
                { ConfigurationOptions.ES_OPERATION_CREATE},
                { ConfigurationOptions.ES_OPERATION_UPDATE }
                });
    }

    public CommandTest(String operation) {
        this.operation = operation;
    }

    @Before
    public void prepare() {
        ba.reset();
        map.put("n", 1);
        map.put("s", "v");
    }

    @Test
    public void testNoHeader() throws Exception {
        assumeFalse(ConfigurationOptions.ES_OPERATION_UPDATE.equals(operation));
        create(settings()).write(map).copyTo(ba);
        String result = prefix() + "}}" + map();
        assertEquals(result, ba.toString());
    }

    @Test
    public void testConstantId() throws Exception {
        Settings settings = settings();
        noId = true;
        settings.setProperty(ConfigurationOptions.ES_MAPPING_ID, "<1>");

        create(settings).write(map).copyTo(ba);
        String result = prefix() + "\"_id\":\"1\"}}" + map();
        assertEquals(result, ba.toString());
    }

    @Test
    public void testParent() throws Exception {
        Settings settings = settings();
        settings.setProperty(ConfigurationOptions.ES_MAPPING_PARENT, "<5>");

        create(settings).write(map).copyTo(ba);
        String result = prefix() + "\"_parent\":\"5\"}}" + map();
        assertEquals(result, ba.toString());
    }

    @Test
    public void testVersion() throws Exception {
        Settings settings = settings();
        settings.setProperty(ConfigurationOptions.ES_MAPPING_VERSION, "<3>");

        create(settings).write(map).copyTo(ba);
        String result = prefix() + "\"_version\":\"3\"}}" + map();
        assertEquals(result, ba.toString());
    }

    @Test
    public void testTtl() throws Exception {
        Settings settings = settings();
        settings.setProperty(ConfigurationOptions.ES_MAPPING_TTL, "<2>");

        create(settings).write(map).copyTo(ba);
        String result = prefix() + "\"_ttl\":\"2\"}}" + map();
        assertEquals(result, ba.toString());
    }

    @Test
    public void testTimestamp() throws Exception {
        Settings settings = settings();
        settings.setProperty(ConfigurationOptions.ES_MAPPING_TIMESTAMP, "<3>");

        create(settings).write(map).copyTo(ba);
        String result = prefix() + "\"_timestamp\":\"3\"}}" + map();
        assertEquals(result, ba.toString());
    }

    @Test
    public void testRouting() throws Exception {
        Settings settings = settings();
        settings.setProperty(ConfigurationOptions.ES_MAPPING_ROUTING, "<4>");

        create(settings).write(map).copyTo(ba);
        String result = prefix() + "\"_routing\":\"4\"}}" + map();
        assertEquals(result, ba.toString());
    }


    @Test
    public void testAll() throws Exception {
        Settings settings = settings();
        settings.setProperty(ConfigurationOptions.ES_MAPPING_ID, "n");
        settings.setProperty(ConfigurationOptions.ES_MAPPING_TTL, "<2>");
        settings.setProperty(ConfigurationOptions.ES_MAPPING_ROUTING, "s");

        create(settings).write(map).copyTo(ba);
        String result = "{\"" + operation + "\":{\"_id\":\"1\",\"_routing\":\"v\",\"_ttl\":\"2\"}}" + map();
        assertEquals(result, ba.toString());
    }

    @Test(expected = EsHadoopIllegalArgumentException.class)
    public void testIdMandatory() throws Exception {
        assumeTrue(ConfigurationOptions.ES_OPERATION_UPDATE.equals(operation));
        Settings set = settings();
        set.setProperty(ConfigurationOptions.ES_MAPPING_ID, "");
        create(set).write(map).copyTo(ba);
    }

    private Command create(Settings settings) {
        settings.setProperty(ConfigurationOptions.ES_WRITE_OPERATION, operation);
        return BulkCommands.create(settings);
    }

    private Settings settings() {
        Settings set = new TestSettings();
        InitializationUtils.setValueWriterIfNotSet(set, JdkValueWriter.class, null);
        InitializationUtils.setFieldExtractorIfNotSet(set, MapFieldExtractor.class, null);
        set.setResourceWrite("foo/bar");
        if (isUpdateOp()) {
            set.setProperty(ConfigurationOptions.ES_MAPPING_ID, "<2>");
        }
        return set;
    }

    private String prefix() {
        StringBuilder sb = new StringBuilder("{\"" + operation + "\":{");
        if (isUpdateOp() && !noId) {
            sb.append("\"_id\":\"2\",");
        }
        return sb.toString();
    }

    private String map() {
        StringBuilder sb = new StringBuilder("\n{");
        if (isUpdateOp()) {
            sb.append("\"doc_as_upsert\":true,\"doc\":{");
        }

        sb.append("\"n\":1,\"s\":\"v\"}");
        if (isUpdateOp()) {
            sb.append("}");
        }
        sb.append("\n");
        return sb.toString();
    }

    private boolean isUpdateOp() {
        return ConfigurationOptions.ES_OPERATION_UPDATE.equals(operation);
    }
}