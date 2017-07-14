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

import org.elasticsearch.hadoop.HdpBootstrap;
import org.elasticsearch.hadoop.QueryTestParams;
import org.elasticsearch.hadoop.cascading.EsTap;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.util.TestUtils;
import org.junit.Test;

import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

public class AbstractCascadingHadoopJsonSaveTest {

    private static final String INPUT = TestUtils.sampleArtistsJson(CascadingHadoopSuite.configuration);

    @Test
    public void testWriteToES() throws Exception {
        // local file-system source
        Tap in = sourceTap();
        Tap out = new EsTap("json-cascading-hadoop-artists/data");

        Pipe pipe = new Pipe("copy");
        build(cfg(), in, out, pipe);
    }

    @Test(expected = Exception.class)
    public void testIndexAutoCreateDisabled() throws Exception {
        Properties cfg = cfg();
        cfg.setProperty(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "false");

        // local file-system source
        Tap in = sourceTap();
        Tap out = new EsTap("json-cascading-hadoop-non-existing/data", new Fields("line"));
        Pipe pipe = new Pipe("copy");
        build(cfg, in, out, pipe);
    }

    @Test
    public void testIndexPattern() throws Exception {
        Tap in = sourceTap();
        Tap out = new EsTap("json-cascading-hadoop-pattern-{tag}/data", new Fields("line"));
        Pipe pipe = new Pipe("copy");
        build(cfg(), in, out, pipe);
    }

    @Test
    public void testIndexPatternWithFormat() throws Exception {
        Tap in = sourceTap();
        Tap out = new EsTap("json-cascading-hadoop-pattern-format-{@timestamp|YYYY-MM-dd}/data", new Fields("line"));
        Pipe pipe = new Pipe("copy");
        build(cfg(), in, out, pipe);
    }

    private void build(Properties cfg, Tap in, Tap out, Pipe pipe) {
        StatsUtils.proxy(new HadoopFlowConnector(cfg).connect(in, out, pipe)).complete();
    }

    private Properties cfg() {
        Properties props = HdpBootstrap.asProperties(QueryTestParams.provisionQueries(CascadingHadoopSuite.configuration));
        props.put(ConfigurationOptions.ES_INPUT_JSON, "true");
        return props;
    }

    private Tap sourceTap() {
        return new Hfs(new TextDelimited(new Fields("line")), INPUT);
    }
}