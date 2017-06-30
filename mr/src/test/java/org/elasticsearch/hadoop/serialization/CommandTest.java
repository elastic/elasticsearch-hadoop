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
import org.elasticsearch.hadoop.serialization.bulk.BulkCommand;
import org.elasticsearch.hadoop.serialization.bulk.BulkCommands;
import org.elasticsearch.hadoop.util.BytesArray;
import org.elasticsearch.hadoop.util.EsMajorVersion;
import org.elasticsearch.hadoop.util.StringUtils;
import org.elasticsearch.hadoop.util.TestSettings;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static org.junit.Assert.assertEquals;

import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

@RunWith(Parameterized.class)
public class CommandTest {

    private final BytesArray ba = new BytesArray(1024);
    private Object data;
    private final String operation;
    private boolean noId = false;
    private boolean jsonInput = false;
    private final EsMajorVersion version;

    @Parameters
    public static Collection<Object[]> data() {

        // make sure all versions are tested. Throw if a new one is seen:
        if (EsMajorVersion.LATEST != EsMajorVersion.V_6_X) {
            throw new IllegalStateException("CommandTest needs new version updates.");
        }

        return Arrays.asList(new Object[][] {
                { ConfigurationOptions.ES_OPERATION_INDEX, false, EsMajorVersion.V_1_X },
                { ConfigurationOptions.ES_OPERATION_CREATE, false, EsMajorVersion.V_1_X },
                { ConfigurationOptions.ES_OPERATION_UPDATE, false, EsMajorVersion.V_1_X },
                { ConfigurationOptions.ES_OPERATION_INDEX, true, EsMajorVersion.V_1_X },
                { ConfigurationOptions.ES_OPERATION_CREATE, true, EsMajorVersion.V_1_X },
                { ConfigurationOptions.ES_OPERATION_UPDATE, true, EsMajorVersion.V_1_X },
                { ConfigurationOptions.ES_OPERATION_INDEX, false, EsMajorVersion.V_2_X },
                { ConfigurationOptions.ES_OPERATION_CREATE, false, EsMajorVersion.V_2_X },
                { ConfigurationOptions.ES_OPERATION_UPDATE, false, EsMajorVersion.V_2_X },
                { ConfigurationOptions.ES_OPERATION_INDEX, true, EsMajorVersion.V_2_X },
                { ConfigurationOptions.ES_OPERATION_CREATE, true, EsMajorVersion.V_2_X },
                { ConfigurationOptions.ES_OPERATION_UPDATE, true, EsMajorVersion.V_2_X },
                { ConfigurationOptions.ES_OPERATION_INDEX, false, EsMajorVersion.V_5_X },
                { ConfigurationOptions.ES_OPERATION_CREATE, false, EsMajorVersion.V_5_X },
                { ConfigurationOptions.ES_OPERATION_UPDATE, false, EsMajorVersion.V_5_X },
                { ConfigurationOptions.ES_OPERATION_INDEX, true, EsMajorVersion.V_5_X },
                { ConfigurationOptions.ES_OPERATION_CREATE, true, EsMajorVersion.V_5_X },
                { ConfigurationOptions.ES_OPERATION_UPDATE, true, EsMajorVersion.V_5_X },
                { ConfigurationOptions.ES_OPERATION_INDEX, false, EsMajorVersion.V_6_X },
                { ConfigurationOptions.ES_OPERATION_CREATE, false, EsMajorVersion.V_6_X },
                { ConfigurationOptions.ES_OPERATION_UPDATE, false, EsMajorVersion.V_6_X },
                { ConfigurationOptions.ES_OPERATION_INDEX, true, EsMajorVersion.V_6_X },
                { ConfigurationOptions.ES_OPERATION_CREATE, true, EsMajorVersion.V_6_X },
                { ConfigurationOptions.ES_OPERATION_UPDATE, true, EsMajorVersion.V_6_X },
        });
    }

    public CommandTest(String operation, boolean jsonInput, EsMajorVersion version) {
        this.operation = operation;
        this.jsonInput = jsonInput;
        this.version = version;
    }

    @Before
    public void prepare() {
        ba.reset();
        if (!jsonInput) {
            Map map = new LinkedHashMap();
            map.put("n", 1);
            map.put("s", "v");
            data = map;
        }
        else {
            data = "{\"n\":1,\"s\":\"v\"}";
        }
    }

    @Test
    public void testNoHeader() throws Exception {
        assumeFalse(ConfigurationOptions.ES_OPERATION_UPDATE.equals(operation));
        create(settings()).write(data).copyTo(ba);
        String result = prefix() + "}}" + map();
        assertEquals(result, ba.toString());
    }

    @Test
    // check user friendliness and escape the string if needed
    public void testConstantId() throws Exception {
        Settings settings = settings();
        noId = true;
        settings.setProperty(ConfigurationOptions.ES_MAPPING_ID, "<foobar>");

        create(settings).write(data).copyTo(ba);
        String result = prefix() + "\"_id\":\"foobar\"}}" + map();

        assertEquals(result, ba.toString());
    }

    @Test
    public void testParent() throws Exception {
        Settings settings = settings();
        settings.setProperty(ConfigurationOptions.ES_MAPPING_PARENT, "<5>");

        create(settings).write(data).copyTo(ba);
        String result = prefix() + "\"_parent\":5}}" + map();
        assertEquals(result, ba.toString());
    }

    @Test
    public void testVersion() throws Exception {
        Settings settings = settings();
        settings.setProperty(ConfigurationOptions.ES_MAPPING_VERSION, "<3>");

        create(settings).write(data).copyTo(ba);
        String result = prefix() + "\"_version\":3,\"_version_type\":\"external\"}}" + map();
        assertEquals(result, ba.toString());
    }

    @Test
    public void testTtl() throws Exception {
        Settings settings = settings();
        settings.setProperty(ConfigurationOptions.ES_MAPPING_TTL, "<2>");

        create(settings).write(data).copyTo(ba);
        String result = prefix() + "\"_ttl\":2}}" + map();
        assertEquals(result, ba.toString());
    }

    @Test
    public void testTimestamp() throws Exception {
        Settings settings = settings();
        settings.setProperty(ConfigurationOptions.ES_MAPPING_TIMESTAMP, "<3>");
        create(settings).write(data).copyTo(ba);
        String result = prefix() + "\"_timestamp\":3}}" + map();
        assertEquals(result, ba.toString());
    }

    @Test
    public void testRouting() throws Exception {
        Settings settings = settings();
        settings.setProperty(ConfigurationOptions.ES_MAPPING_ROUTING, "<4>");

        create(settings).write(data).copyTo(ba);
        String result = prefix() + "\"_routing\":4}}" + map();
        assertEquals(result, ba.toString());
    }

    @Test
    public void testAll() throws Exception {
        Settings settings = settings();
        settings.setProperty(ConfigurationOptions.ES_MAPPING_ID, "n");
        settings.setProperty(ConfigurationOptions.ES_MAPPING_TTL, "<2>");
        settings.setProperty(ConfigurationOptions.ES_MAPPING_ROUTING, "s");

        create(settings).write(data).copyTo(ba);
        String result = "{\"" + operation + "\":{\"_id\":1,\"_routing\":\"v\",\"_ttl\":2}}" + map();
        assertEquals(result, ba.toString());
    }

    @Test
    public void testIdPattern() throws Exception {
        Settings settings = settings();
        settings.setResourceWrite("foo/{n}");

        create(settings).write(data).copyTo(ba);
        String result = "{\"" + operation + "\":{\"_index\":\"foo\",\"_type\":\"1\"" + (isUpdateOp() ? ",\"_id\":2" : "") + "}}" + map();
        assertEquals(result, ba.toString());
    }

    @Test(expected = EsHadoopIllegalArgumentException.class)
    public void testIdMandatory() throws Exception {
        assumeTrue(ConfigurationOptions.ES_OPERATION_UPDATE.equals(operation));
        Settings set = settings();
        set.setProperty(ConfigurationOptions.ES_MAPPING_ID, "");
        create(set).write(data).copyTo(ba);
    }

    @Test
    public void testUpdateOnlyInlineScript5X() throws Exception {
        assumeTrue(ConfigurationOptions.ES_OPERATION_UPDATE.equals(operation));
        assumeTrue(version.after(EsMajorVersion.V_1_X));
        assumeTrue(version.before(EsMajorVersion.V_6_X));
        Settings set = settings();

        set.setProperty(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "yes");
        set.setProperty(ConfigurationOptions.ES_UPDATE_RETRY_ON_CONFLICT, "3");
        set.setProperty(ConfigurationOptions.ES_UPDATE_SCRIPT_INLINE, "counter = 3");
        set.setProperty(ConfigurationOptions.ES_UPDATE_SCRIPT_LANG, "groovy");

        create(set).write(data).copyTo(ba);
        String result =
                "{\"" + operation + "\":{\"_id\":2,\"_retry_on_conflict\":3}}\n" +
                "{\"script\":{\"inline\":\"counter = 3\",\"lang\":\"groovy\"}}\n";
        assertEquals(result, ba.toString());
    }

    @Test
    public void testUpdateOnlyInlineScript6X() throws Exception {
        assumeTrue(ConfigurationOptions.ES_OPERATION_UPDATE.equals(operation));
        assumeTrue(version.onOrAfter(EsMajorVersion.V_6_X));
        Settings set = settings();

        set.setProperty(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "yes");
        set.setProperty(ConfigurationOptions.ES_UPDATE_RETRY_ON_CONFLICT, "3");
        set.setProperty(ConfigurationOptions.ES_UPDATE_SCRIPT_INLINE, "counter = 3");
        set.setProperty(ConfigurationOptions.ES_UPDATE_SCRIPT_LANG, "groovy");

        create(set).write(data).copyTo(ba);
        String result =
                "{\"" + operation + "\":{\"_id\":2,\"_retry_on_conflict\":3}}\n" +
                        "{\"script\":{\"source\":\"counter = 3\",\"lang\":\"groovy\"}}\n";
        assertEquals(result, ba.toString());
    }

    @Test
    public void testUpdateOnlyFileScript5X() throws Exception {
        assumeTrue(ConfigurationOptions.ES_OPERATION_UPDATE.equals(operation));
        assumeTrue(version.after(EsMajorVersion.V_1_X));
        Settings set = settings();

        set.setProperty(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "yes");
        set.setProperty(ConfigurationOptions.ES_UPDATE_RETRY_ON_CONFLICT, "3");
        set.setProperty(ConfigurationOptions.ES_UPDATE_SCRIPT_FILE, "set_count");
        set.setProperty(ConfigurationOptions.ES_UPDATE_SCRIPT_LANG, "groovy");

        create(set).write(data).copyTo(ba);
        String result =
                "{\"" + operation + "\":{\"_id\":2,\"_retry_on_conflict\":3}}\n" +
                        "{\"script\":{\"file\":\"set_count\",\"lang\":\"groovy\"}}\n";
        assertEquals(result, ba.toString());
    }

    @Test
    public void testUpdateOnlyInlineScript1X() throws Exception {
        assumeTrue(ConfigurationOptions.ES_OPERATION_UPDATE.equals(operation));
        assumeTrue(version.onOrBefore(EsMajorVersion.V_1_X));
        Settings set = settings();

        set.setProperty(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "yes");
        set.setProperty(ConfigurationOptions.ES_UPDATE_RETRY_ON_CONFLICT, "3");
        set.setProperty(ConfigurationOptions.ES_UPDATE_SCRIPT_INLINE, "counter = 3");
        set.setProperty(ConfigurationOptions.ES_UPDATE_SCRIPT_LANG, "groovy");

        create(set).write(data).copyTo(ba);
        String result =
                "{\"" + operation + "\":{\"_id\":2,\"_retry_on_conflict\":3}}\n" +
                "{\"lang\":\"groovy\",\"script\":\"counter = 3\"}\n";
        assertEquals(result, ba.toString());
    }

    @Test
    public void testUpdateOnlyFileScript1X() throws Exception {
        assumeTrue(ConfigurationOptions.ES_OPERATION_UPDATE.equals(operation));
        assumeTrue(version.onOrBefore(EsMajorVersion.V_1_X));
        Settings set = settings();

        set.setProperty(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "yes");
        set.setProperty(ConfigurationOptions.ES_UPDATE_RETRY_ON_CONFLICT, "3");
        set.setProperty(ConfigurationOptions.ES_UPDATE_SCRIPT_FILE, "set_count");
        set.setProperty(ConfigurationOptions.ES_UPDATE_SCRIPT_LANG, "groovy");

        create(set).write(data).copyTo(ba);
        String result =
                "{\"" + operation + "\":{\"_id\":2,\"_retry_on_conflict\":3}}\n" +
                        "{\"lang\":\"groovy\",\"script_file\":\"set_count\"}\n";
        assertEquals(result, ba.toString());
    }

    @Test
    public void testUpdateOnlyParamInlineScript5X() throws Exception {
        assumeTrue(ConfigurationOptions.ES_OPERATION_UPDATE.equals(operation));
        assumeTrue(version.after(EsMajorVersion.V_1_X));
        assumeTrue(version.before(EsMajorVersion.V_6_X));
        Settings set = settings();

        set.setProperty(ConfigurationOptions.ES_MAPPING_ID, "n");
        set.setProperty(ConfigurationOptions.ES_UPDATE_SCRIPT_INLINE, "counter = param1; anothercounter = param2");
        set.setProperty(ConfigurationOptions.ES_UPDATE_SCRIPT_LANG, "groovy");
        set.setProperty(ConfigurationOptions.ES_UPDATE_SCRIPT_PARAMS, " param1:<1>,   param2:n ");

        create(set).write(data).copyTo(ba);

        String result =
                "{\"" + operation + "\":{\"_id\":1}}\n" +
                "{\"script\":{\"inline\":\"counter = param1; anothercounter = param2\",\"lang\":\"groovy\",\"params\":{\"param1\":1,\"param2\":1}}}\n";

        assertEquals(result, ba.toString());
    }

    @Test
    public void testUpdateOnlyParamInlineScript6X() throws Exception {
        assumeTrue(ConfigurationOptions.ES_OPERATION_UPDATE.equals(operation));
        assumeTrue(version.onOrAfter(EsMajorVersion.V_6_X));
        Settings set = settings();

        set.setProperty(ConfigurationOptions.ES_MAPPING_ID, "n");
        set.setProperty(ConfigurationOptions.ES_UPDATE_SCRIPT_INLINE, "counter = param1; anothercounter = param2");
        set.setProperty(ConfigurationOptions.ES_UPDATE_SCRIPT_LANG, "groovy");
        set.setProperty(ConfigurationOptions.ES_UPDATE_SCRIPT_PARAMS, " param1:<1>,   param2:n ");

        create(set).write(data).copyTo(ba);

        String result =
                "{\"" + operation + "\":{\"_id\":1}}\n" +
                        "{\"script\":{\"source\":\"counter = param1; anothercounter = param2\",\"lang\":\"groovy\",\"params\":{\"param1\":1,\"param2\":1}}}\n";

        assertEquals(result, ba.toString());
    }

    @Test
    public void testUpdateOnlyParamFileScript5X() throws Exception {
        assumeTrue(ConfigurationOptions.ES_OPERATION_UPDATE.equals(operation));
        assumeTrue(version.after(EsMajorVersion.V_1_X));
        Settings set = settings();

        set.setProperty(ConfigurationOptions.ES_MAPPING_ID, "n");
        set.setProperty(ConfigurationOptions.ES_UPDATE_SCRIPT_FILE, "set_counter");
        set.setProperty(ConfigurationOptions.ES_UPDATE_SCRIPT_LANG, "groovy");
        set.setProperty(ConfigurationOptions.ES_UPDATE_SCRIPT_PARAMS, " param1:<1>,   param2:n ");

        create(set).write(data).copyTo(ba);

        String result =
                "{\"" + operation + "\":{\"_id\":1}}\n" +
                        "{\"script\":{\"file\":\"set_counter\",\"lang\":\"groovy\",\"params\":{\"param1\":1,\"param2\":1}}}\n";

        assertEquals(result, ba.toString());
    }

    @Test
    public void testUpdateOnlyParamInlineScript1X() throws Exception {
        assumeTrue(ConfigurationOptions.ES_OPERATION_UPDATE.equals(operation));
        assumeTrue(version.onOrBefore(EsMajorVersion.V_1_X));
        Settings set = settings();

        set.setProperty(ConfigurationOptions.ES_MAPPING_ID, "n");
        set.setProperty(ConfigurationOptions.ES_UPDATE_SCRIPT_INLINE, "counter = param1; anothercounter = param2");
        set.setProperty(ConfigurationOptions.ES_UPDATE_SCRIPT_LANG, "groovy");
        set.setProperty(ConfigurationOptions.ES_UPDATE_SCRIPT_PARAMS, " param1:<1>,   param2:n ");

        create(set).write(data).copyTo(ba);

        String result =
                "{\"" + operation + "\":{\"_id\":1}}\n" +
                "{\"params\":{\"param1\":1,\"param2\":1},\"lang\":\"groovy\",\"script\":\"counter = param1; anothercounter = param2\"}\n";
        assertEquals(result, ba.toString());
    }

    @Test
    public void testUpdateOnlyParamFileScript1X() throws Exception {
        assumeTrue(ConfigurationOptions.ES_OPERATION_UPDATE.equals(operation));
        assumeTrue(version.onOrBefore(EsMajorVersion.V_1_X));
        Settings set = settings();

        set.setProperty(ConfigurationOptions.ES_MAPPING_ID, "n");
        set.setProperty(ConfigurationOptions.ES_UPDATE_SCRIPT_FILE, "set_counter");
        set.setProperty(ConfigurationOptions.ES_UPDATE_SCRIPT_LANG, "groovy");
        set.setProperty(ConfigurationOptions.ES_UPDATE_SCRIPT_PARAMS, " param1:<1>,   param2:n ");

        create(set).write(data).copyTo(ba);

        String result =
                "{\"" + operation + "\":{\"_id\":1}}\n" +
                        "{\"params\":{\"param1\":1,\"param2\":1},\"lang\":\"groovy\",\"script_file\":\"set_counter\"}\n";
        assertEquals(result, ba.toString());
    }

    private BulkCommand create(Settings settings) {
        if (!StringUtils.hasText(settings.getResourceWrite())) {
            settings.setProperty(ConfigurationOptions.ES_WRITE_OPERATION, operation);
        }
        return BulkCommands.create(settings, null, version);
    }

    private Settings settings() {
        Settings set = new TestSettings();

        set.setProperty(ConfigurationOptions.ES_INPUT_JSON, Boolean.toString(jsonInput));


        InitializationUtils.setValueWriterIfNotSet(set, JdkValueWriter.class, null);
        InitializationUtils.setFieldExtractorIfNotSet(set, MapFieldExtractor.class, null);
        InitializationUtils.setBytesConverterIfNeeded(set, JdkBytesConverter.class, null);

        set.setProperty(ConfigurationOptions.ES_WRITE_OPERATION, operation);
        set.setResourceWrite("foo/bar");
        if (isUpdateOp()) {
            set.setProperty(ConfigurationOptions.ES_MAPPING_ID, "<2>");
        }
        return set;
    }

    private String prefix() {
        StringBuilder sb = new StringBuilder("{\"" + operation + "\":{");
        if (isUpdateOp() && !noId) {
            sb.append("\"_id\":2,");
        }
        return sb.toString();
    }

    private String map() {
        StringBuilder sb = new StringBuilder("\n{");
        if (isUpdateOp()) {
            sb.append("\"doc\":{");
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