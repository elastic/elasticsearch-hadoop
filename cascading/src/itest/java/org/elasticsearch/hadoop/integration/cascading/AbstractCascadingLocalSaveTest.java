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
import org.elasticsearch.hadoop.mr.RestUtils;
import org.elasticsearch.hadoop.util.EsMajorVersion;
import org.elasticsearch.hadoop.util.TestSettings;
import org.elasticsearch.hadoop.util.TestUtils;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import cascading.cascade.CascadeConnector;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.local.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;

import static org.elasticsearch.hadoop.util.EsMajorVersion.V_5_X;
import static org.junit.Assert.assertThat;

import static org.hamcrest.CoreMatchers.is;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class AbstractCascadingLocalSaveTest {

    private static final String INPUT = TestUtils.sampleArtistsDat();
    private static final EsMajorVersion VERSION = TestUtils.getEsVersion();

    @Test
    public void testWriteToES() throws Exception {
        // local file-system source
        Tap in = sourceTap();
        Tap out = new EsTap("cascading-local-artists/data", new Fields("name", "url", "picture"));

        Pipe pipe = new Pipe("copy");
        build(new TestSettings().getProperties(), in, out, pipe);
    }

    @Test
    public void testWriteToESMapping() throws Exception {
        assertThat(RestUtils.getMapping("cascading-local-artists/data").toString(),
                VERSION.onOrAfter(V_5_X)
                        ? is("data=[name=TEXT, picture=TEXT, url=TEXT]")
                        : is("data=[name=STRING, picture=STRING, url=STRING]"));
    }

    @Test
    public void testWriteToESWithAlias() throws Exception {
        // local file-system source
        Tap in = sourceTap();
        Tap out = new EsTap("cascading-local-alias/data", new Fields("name", "url", "picture"));
        Pipe pipe = new Pipe("copy");

        // rename "id" -> "garbage"
        pipe = new Each(pipe, new Identity(new Fields("garbage", "name", "url", "picture", "ts", "tag")));

        Properties props = new TestSettings().getProperties();
        props.setProperty("es.mapping.names", "url:address");
        build(props, in, out, pipe);
    }

    @Test
    public void testWriteToESWithAliasMapping() throws Exception {
        assertThat(RestUtils.getMapping("cascading-local-alias/data").toString(),
                VERSION.onOrAfter(V_5_X)
                        ? is("data=[address=TEXT, name=TEXT, picture=TEXT]")
                        : is("data=[address=STRING, name=STRING, picture=STRING]"));
    }


    @Test(expected = Exception.class)
    public void testIndexAutoCreateDisabled() throws Exception {
        Properties properties = new TestSettings().getProperties();
        properties.setProperty(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "false");

        // local file-system source
        Tap in = sourceTap();
        Tap out = new EsTap("cascading-local-non-existing/data", new Fields("name", "url", "picture"));

        Pipe pipe = new Pipe("copy");

        // rename "id" -> "garbage"
        pipe = new Each(pipe, new Identity(new Fields("garbage", "name", "url", "picture", "ts")));
        build(properties, in, out, pipe);
    }

    @Test
    public void testFieldMapping() throws Exception {
        // local file-system source
        Tap in = sourceTap();
        Tap out = new EsTap("cascading-local-fieldmapping/data", new Fields("id", "name", "url", "picture"));
        Pipe pipe = new Pipe("copy");

        // rename "ts" -> "garbage"
        pipe = new Each(pipe, new Identity(new Fields("id", "name", "url", "picture", "garbage", "tag")));

        Properties props = new TestSettings().getProperties();
        props.setProperty("es.mapping.id", "id");
        props.setProperty("es.mapping.version", "<5>");
        build(props, in, out, pipe);
    }

    @Test
    public void testWriteToESWithtestFieldMappingMapping() throws Exception {
        assertThat(RestUtils.getMapping("cascading-local-fieldmapping/data").toString(),
                VERSION.onOrAfter(V_5_X)
                        ? is("data=[id=TEXT, name=TEXT, picture=TEXT, url=TEXT]")
                        : is("data=[id=STRING, name=STRING, picture=STRING, url=STRING]"));
    }

    @Test
    public void testIndexPattern() throws Exception {
        Properties properties = new TestSettings().getProperties();

        // local file-system source
        Tap in = sourceTap();
        Tap out = new EsTap("cascading-local-pattern-{tag}/data", new Fields("id", "name", "url", "picture", "tag"));
        Pipe pipe = new Pipe("copy");
        build(properties, in, out, pipe);
    }

    @Test
    public void testIndexPatternMapping() throws Exception {
        assertThat(RestUtils.getMapping("cascading-local-pattern-5/data").toString(),
                VERSION.onOrAfter(V_5_X)
                        ? is("data=[id=TEXT, name=TEXT, picture=TEXT, tag=TEXT, url=TEXT]")
                        : is("data=[id=STRING, name=STRING, picture=STRING, tag=STRING, url=STRING]"));
    }

    @Test
    public void testIndexPatternWithFormatAndAlias() throws Exception {
        Properties properties = new TestSettings().getProperties();

        // local file-system source
        Tap in = sourceTap();
        Tap out = new EsTap("cascading-local-pattern-format-{ts|YYYY-MM-dd}/data", new Fields("id", "name", "url", "picture", "ts"));
        Pipe pipe = new Pipe("copy");

        build(properties, in, out, pipe);
    }

    @Test
    public void testIndexPatternWithFormatAndAliasMapping() throws Exception {
        assertThat(RestUtils.getMapping("cascading-local-pattern-format-2017-10-06/data").toString(),
                VERSION.onOrAfter(V_5_X)
                        ? is("data=[id=TEXT, name=TEXT, picture=TEXT, ts=DATE, url=TEXT]")
                        : is("data=[id=STRING, name=STRING, picture=STRING, ts=DATE, url=STRING]"));
    }

    @Test
    public void testUpdate() throws Exception {
        // local file-system source
        Tap in = sourceTap();
        Tap out = new EsTap("cascading-local-createwithid/data", new Fields("id", "name", "url", "picture"));
        Properties props = new TestSettings().getProperties();
        props.put(ConfigurationOptions.ES_MAPPING_ID, "id");

        Pipe pipe = new Pipe("copy");
        build(props, in, out, pipe);
    }


    @Test
    public void testUpdateOnlyScript() throws Exception {
        Properties properties = new TestSettings().getProperties();
        properties.put(ConfigurationOptions.ES_WRITE_OPERATION, "update");
        properties.put(ConfigurationOptions.ES_MAPPING_ID, "id");
        properties.put(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "yes");
        properties.put(ConfigurationOptions.ES_UPDATE_RETRY_ON_CONFLICT, "3");

        if (VERSION.onOrAfter(EsMajorVersion.V_5_X)) {
            properties.put(ConfigurationOptions.ES_UPDATE_SCRIPT, "int counter = 3");
            properties.put(ConfigurationOptions.ES_UPDATE_SCRIPT_LANG, "painless");
        } else {
            properties.put(ConfigurationOptions.ES_UPDATE_SCRIPT, "counter = 3");
            properties.put(ConfigurationOptions.ES_UPDATE_SCRIPT_LANG, "groovy");
        }

        Tap in = sourceTap();
        // use an existing id to allow the update to succeed
        Tap out = new EsTap("cascading-local-createwithid/data", new Fields("id", "name", "url", "picture"));

        Pipe pipe = new Pipe("copy");
        build(properties, in, out, pipe);
    }

    @Test
    public void testUpdateOnlyParamScript() throws Exception {
        Properties properties = new TestSettings().getProperties();
        properties.put(ConfigurationOptions.ES_WRITE_OPERATION, "update");
        properties.put(ConfigurationOptions.ES_MAPPING_ID, "id");
        properties.put(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "yes");
        properties.put(ConfigurationOptions.ES_UPDATE_RETRY_ON_CONFLICT, "3");
        properties.put(ConfigurationOptions.ES_UPDATE_SCRIPT_PARAMS, " param1:<1>,   param2:id ");

        if (VERSION.onOrAfter(EsMajorVersion.V_5_X)) {
            properties.put(ConfigurationOptions.ES_UPDATE_SCRIPT, "int counter = params.param1; String anothercounter = params.param2");
            properties.put(ConfigurationOptions.ES_UPDATE_SCRIPT_LANG, "painless");
        } else {
            properties.put(ConfigurationOptions.ES_UPDATE_SCRIPT, "counter = param1; anothercounter = param2");
            properties.put(ConfigurationOptions.ES_UPDATE_SCRIPT_LANG, "groovy");
        }

        Tap in = sourceTap();
        // use an existing id to allow the update to succeed
        Tap out = new EsTap("cascading-local-createwithid/data", new Fields("id", "name", "url", "picture"));

        Pipe pipe = new Pipe("copy");
        build(properties, in, out, pipe);
    }

    @Test
    public void testUpdateOnlyParamJsonScript() throws Exception {
        Properties properties = new TestSettings().getProperties();
        properties.put(ConfigurationOptions.ES_WRITE_OPERATION, "update");
        properties.put(ConfigurationOptions.ES_MAPPING_ID, "id");
        properties.put(ConfigurationOptions.ES_UPDATE_RETRY_ON_CONFLICT, "3");
        properties.put(ConfigurationOptions.ES_UPDATE_SCRIPT_PARAMS_JSON, "{ \"param1\":1, \"param2\":2}");

        if(VERSION.onOrAfter(EsMajorVersion.V_5_X)) {
            properties.put(ConfigurationOptions.ES_UPDATE_SCRIPT, "int counter = params.param1; int anothercounter = params.param2");
            properties.put(ConfigurationOptions.ES_UPDATE_SCRIPT_LANG, "painless");
        } else {
            properties.put(ConfigurationOptions.ES_UPDATE_SCRIPT, "counter = param1; anothercounter = param2");
            properties.put(ConfigurationOptions.ES_UPDATE_SCRIPT_LANG, "groovy");
        }

        Tap in = sourceTap();
        // use an existing id to allow the update to succeed
        Tap out = new EsTap("cascading-local-createwithid/data", new Fields("id", "name", "url", "picture"));

        Pipe pipe = new Pipe("copy");
        build(properties, in, out, pipe);
    }


    @Test
    public void testUpsert() throws Exception {
        Properties properties = new TestSettings().getProperties();
        properties.put(ConfigurationOptions.ES_WRITE_OPERATION, "upsert");
        properties.put(ConfigurationOptions.ES_MAPPING_ID, "id");

        Tap in = sourceTap();
        Tap out = new EsTap("cascading-local-upsert/data", new Fields("id", "name", "url", "picture"));

        Pipe pipe = new Pipe("copy");
        build(properties, in, out, pipe);
    }

    @Test
    public void testUpsertScript() throws Exception {
        Properties properties = new TestSettings().getProperties();
        properties.put(ConfigurationOptions.ES_WRITE_OPERATION, "upsert");
        properties.put(ConfigurationOptions.ES_MAPPING_ID, "id");

        if (VERSION.onOrAfter(EsMajorVersion.V_5_X)) {
            properties.put(ConfigurationOptions.ES_UPDATE_SCRIPT, "int counter = 1");
            properties.put(ConfigurationOptions.ES_UPDATE_SCRIPT_LANG, "painless");
        } else {
            properties.put(ConfigurationOptions.ES_UPDATE_SCRIPT, "counter = 1");
            properties.put(ConfigurationOptions.ES_UPDATE_SCRIPT_LANG, "groovy");
        }

        Tap in = sourceTap();
        // use an existing id to allow the update to succeed
        Tap out = new EsTap("cascading-local-upsert-script/data", new Fields("id", "name", "url", "picture"));

        Pipe pipe = new Pipe("copy");
        build(properties, in, out, pipe);
    }

    @Test
    public void testUpsertParamScript() throws Exception {
        Properties properties = new TestSettings().getProperties();
        properties.put(ConfigurationOptions.ES_WRITE_OPERATION, "upsert");
        properties.put(ConfigurationOptions.ES_MAPPING_ID, "id");
        properties.put(ConfigurationOptions.ES_UPDATE_SCRIPT_PARAMS, " param1:<1>,   param2:id ");

        if (VERSION.onOrAfter(EsMajorVersion.V_5_X)) {
            properties.put(ConfigurationOptions.ES_UPDATE_SCRIPT, "int counter = params.param1; int anothercounter = Integer.parseInt(params.param2)");
            properties.put(ConfigurationOptions.ES_UPDATE_SCRIPT_LANG, "painless");
        } else {
            properties.put(ConfigurationOptions.ES_UPDATE_SCRIPT, "counter += param1; anothercounter += param2");
            properties.put(ConfigurationOptions.ES_UPDATE_SCRIPT_LANG, "groovy");
        }

        Tap in = sourceTap();
        // use an existing id to allow the update to succeed
        Tap out = new EsTap("cascading-local-upsert-param-script/data", new Fields("id", "name", "url", "picture"));

        Pipe pipe = new Pipe("copy");
        build(properties, in, out, pipe);
    }

    @Test
    public void testUpsertParamJsonScript() throws Exception {
        Properties properties = new TestSettings().getProperties();
        properties.put(ConfigurationOptions.ES_WRITE_OPERATION, "upsert");
        properties.put(ConfigurationOptions.ES_MAPPING_ID, "id");
        properties.put(ConfigurationOptions.ES_UPDATE_SCRIPT_PARAMS_JSON, "{ \"param1\":1, \"param2\":2}");

        if (VERSION.onOrAfter(EsMajorVersion.V_5_X)) {
            properties.put(ConfigurationOptions.ES_UPDATE_SCRIPT, "int counter = params.param1; int anothercounter = Integer.parseInt(params.param2)");
            properties.put(ConfigurationOptions.ES_UPDATE_SCRIPT_LANG, "painless");
        } else {
            properties.put(ConfigurationOptions.ES_UPDATE_SCRIPT, "counter += param1; anothercounter += param2");
            properties.put(ConfigurationOptions.ES_UPDATE_SCRIPT_LANG, "groovy");
        }

        Tap in = sourceTap();
        // use an existing id to allow the update to succeed
        Tap out = new EsTap("cascading-local-upsert-script-json-script/data", new Fields("id", "name", "url", "picture"));

        Pipe pipe = new Pipe("copy");
        build(properties, in, out, pipe);
    }

    @Test
    public void testCascadeConnector() {
        Pipe copy = new Pipe("copy");
        Properties cfg = new TestSettings().getProperties();

        FlowDef flow = new FlowDef().addSource(copy, sourceTap()).addTailSink(copy,
                new EsTap("cascading-local-cascade-connector/data"));

        FlowConnector connector = new LocalFlowConnector(cfg);
        Flow[] flows = new Flow[] { connector.connect(flow) };

        CascadeConnector cascadeConnector = new CascadeConnector(cfg);
        cascadeConnector.connect(flows).complete();
    }

    private Tap sourceTap() {
        return new FileTap(new TextDelimited(new Fields("id", "name", "url", "picture", "ts", "tag")), INPUT);
    }

    private void build(Properties cfg, Tap in, Tap out, Pipe pipe) {
        StatsUtils.proxy(new LocalFlowConnector(cfg).connect(in, out, pipe)).complete();
    }
}