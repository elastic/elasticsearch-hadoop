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
package org.elasticsearch.hadoop.integration.cascading;

import java.util.Properties;

import org.elasticsearch.hadoop.cascading.EsTap;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.util.TestSettings;
import org.elasticsearch.hadoop.util.TestUtils;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import cascading.flow.local.LocalFlowConnector;
import cascading.pipe.Pipe;
import cascading.scheme.local.TextLine;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class AbstractCascadingLocalJsonSaveTest {

    @Test
    public void testWriteToES() throws Exception {
        Properties props = new TestSettings().getProperties();
        props.put(ConfigurationOptions.ES_INPUT_JSON, "true");

        Tap in = sourceTap();
        Tap out = new EsTap("json-cascading-local/artists");

        Pipe pipe = new Pipe("copy");
        build(props, in, out, pipe);
    }

    @Test(expected = Exception.class)
    public void testIndexAutoCreateDisabled() throws Exception {
        Properties properties = new TestSettings().getProperties();
        properties.put(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "false");
        properties.put(ConfigurationOptions.ES_INPUT_JSON, "true");

        Tap in = sourceTap();
        Tap out = new EsTap("json-cascading-local/non-existing", new Fields("line"));
        Pipe pipe = new Pipe("copy");
        build(properties, in, out, pipe);
    }

    @Test
    public void testIndexPattern() throws Exception {
        Properties properties = new TestSettings().getProperties();
        properties.put(ConfigurationOptions.ES_INPUT_JSON, "yes");

        Tap in = sourceTap();
        Tap out = new EsTap("json-cascading-local/pattern-{number}", new Fields("line"));
        Pipe pipe = new Pipe("copy");
        build(properties, in, out, pipe);
    }

    @Test
    public void testIndexPatternWithFormat() throws Exception {
        Properties properties = new TestSettings().getProperties();
        properties.put(ConfigurationOptions.ES_INPUT_JSON, "yes");

        Tap in = sourceTap();
        Tap out = new EsTap("json-cascading-local/pattern-format-{@timestamp:YYYY-MM-dd}", new Fields("line"));
        Pipe pipe = new Pipe("copy");
        build(properties, in, out, pipe);
    }


    @Test
    public void testUpdate() throws Exception {
        // local file-system source
        Tap in = sourceTap();
        Tap out = new EsTap("json-cascading-local/createwithid", new Fields("line"));
        Properties props = new TestSettings().getProperties();
        props.put(ConfigurationOptions.ES_MAPPING_ID, "number");
        props.put(ConfigurationOptions.ES_INPUT_JSON, "yes");

        Pipe pipe = new Pipe("copy");
        build(props, in, out, pipe);
    }


    @Test
    public void testUpdateOnlyScript() throws Exception {
        Properties properties = new TestSettings().getProperties();
        properties.put(ConfigurationOptions.ES_WRITE_OPERATION, "update");
        properties.put(ConfigurationOptions.ES_MAPPING_ID, "number");
        properties.put(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "yes");
        properties.put(ConfigurationOptions.ES_UPDATE_RETRY_ON_CONFLICT, "3");
        properties.put(ConfigurationOptions.ES_UPDATE_SCRIPT, "counter = 3");
        properties.put(ConfigurationOptions.ES_UPDATE_SCRIPT_LANG, "groovy");
        properties.put(ConfigurationOptions.ES_INPUT_JSON, "yes");

        Tap in = sourceTap();
        // use an existing id to allow the update to succeed
        Tap out = new EsTap("json-cascading-local/createwithid", new Fields("line"));

        Pipe pipe = new Pipe("copy");
        build(properties, in, out, pipe);
    }

    @Test
    public void testUpdateOnlyParamScript() throws Exception {
        Properties properties = new TestSettings().getProperties();
        properties.put(ConfigurationOptions.ES_WRITE_OPERATION, "update");
        properties.put(ConfigurationOptions.ES_MAPPING_ID, "number");
        properties.put(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "yes");
        properties.put(ConfigurationOptions.ES_UPDATE_RETRY_ON_CONFLICT, "3");
        properties.put(ConfigurationOptions.ES_UPDATE_SCRIPT, "counter = param1; anothercounter = param2");
        properties.put(ConfigurationOptions.ES_UPDATE_SCRIPT_LANG, "groovy");
        properties.put(ConfigurationOptions.ES_UPDATE_SCRIPT_PARAMS, " param1:<1>,   param2:number ");
        properties.put(ConfigurationOptions.ES_INPUT_JSON, "yes");

        Tap in = sourceTap();
        // use an existing id to allow the update to succeed
        Tap out = new EsTap("json-cascading-local/createwithid", new Fields("line"));

        Pipe pipe = new Pipe("copy");
        build(properties, in, out, pipe);
    }

    @Test
    public void testUpdateOnlyParamJsonScript() throws Exception {
        Properties properties = new TestSettings().getProperties();
        properties.put(ConfigurationOptions.ES_WRITE_OPERATION, "update");
        properties.put(ConfigurationOptions.ES_MAPPING_ID, "number");
        properties.put(ConfigurationOptions.ES_UPDATE_RETRY_ON_CONFLICT, "3");

        properties.put(ConfigurationOptions.ES_UPDATE_SCRIPT, "counter = param1; anothercounter = param2");
        properties.put(ConfigurationOptions.ES_UPDATE_SCRIPT_LANG, "groovy");
        properties.put(ConfigurationOptions.ES_UPDATE_SCRIPT_PARAMS_JSON, "{ \"param1\":1, \"param2\":2}");
        properties.put(ConfigurationOptions.ES_INPUT_JSON, "yes");

        Tap in = sourceTap();
        // use an existing id to allow the update to succeed
        Tap out = new EsTap("json-cascading-local/createwithid", new Fields("line"));

        Pipe pipe = new Pipe("copy");
        build(properties, in, out, pipe);
    }


    @Test
    public void testUpsert() throws Exception {
        Properties properties = new TestSettings().getProperties();
        properties.put(ConfigurationOptions.ES_WRITE_OPERATION, "upsert");
        properties.put(ConfigurationOptions.ES_MAPPING_ID, "number");
        properties.put(ConfigurationOptions.ES_INPUT_JSON, "yes");

        Tap in = sourceTap();
        Tap out = new EsTap("json-cascading-local/upsert", new Fields("line"));

        Pipe pipe = new Pipe("copy");
        build(properties, in, out, pipe);
    }

    @Test
    public void testUpsertScript() throws Exception {
        Properties properties = new TestSettings().getProperties();
        properties.put(ConfigurationOptions.ES_WRITE_OPERATION, "upsert");
        properties.put(ConfigurationOptions.ES_MAPPING_ID, "number");
        properties.put(ConfigurationOptions.ES_INPUT_JSON, "yes");
        properties.put(ConfigurationOptions.ES_UPDATE_SCRIPT, "counter = 1");

        Tap in = sourceTap();
        // use an existing id to allow the update to succeed
        Tap out = new EsTap("json-cascading-local/upsert-script", new Fields("line"));

        Pipe pipe = new Pipe("copy");
        build(properties, in, out, pipe);
    }

    @Test
    public void testUpsertParamScript() throws Exception {
        Properties properties = new TestSettings().getProperties();
        properties.put(ConfigurationOptions.ES_WRITE_OPERATION, "upsert");
        properties.put(ConfigurationOptions.ES_MAPPING_ID, "number");
        properties.put(ConfigurationOptions.ES_INPUT_JSON, "yes");
        properties.put(ConfigurationOptions.ES_UPDATE_SCRIPT, "counter += param1; anothercounter += param2");
        properties.put(ConfigurationOptions.ES_UPDATE_SCRIPT_LANG, "groovy");
        properties.put(ConfigurationOptions.ES_UPDATE_SCRIPT_PARAMS, " param1:<1>,   param2:number ");

        Tap in = sourceTap();
        // use an existing id to allow the update to succeed
        Tap out = new EsTap("json-cascading-local/upsert-param-script", new Fields("line"));

        Pipe pipe = new Pipe("copy");
        build(properties, in, out, pipe);
    }

    @Test
    public void testUpsertParamJsonScript() throws Exception {
        Properties properties = new TestSettings().getProperties();
        properties.put(ConfigurationOptions.ES_WRITE_OPERATION, "upsert");
        properties.put(ConfigurationOptions.ES_MAPPING_ID, "number");
        properties.put(ConfigurationOptions.ES_INPUT_JSON, "yes");
        properties.put(ConfigurationOptions.ES_UPDATE_SCRIPT, "ctx._source.counter += param1; ctx._source.anothercounter += param2");
        properties.put(ConfigurationOptions.ES_UPDATE_SCRIPT_LANG, "groovy");
        properties.put(ConfigurationOptions.ES_UPDATE_SCRIPT_PARAMS_JSON, "{ \"param1\":1, \"param2\":2}");

        Tap in = sourceTap();
        // use an existing id to allow the update to succeed
        Tap out = new EsTap("json-cascading-local/upsert-script-json-script", new Fields("line"));

        Pipe pipe = new Pipe("copy");
        build(properties, in, out, pipe);
    }

    private void build(Properties props, Tap in, Tap out, Pipe pipe) {
        StatsUtils.proxy(new LocalFlowConnector(props).connect(in, out, pipe)).complete();
    }

    private Tap sourceTap() {
        return new FileTap(new TextLine(new Fields("line")), TestUtils.sampleArtistsJson());
    }
}