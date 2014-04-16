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
import org.elasticsearch.hadoop.cascading.EsTap;
import org.elasticsearch.hadoop.util.TestUtils;
import org.junit.Test;

import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

public class AbstractCascadingHadoopSaveTest {

    private static final String INPUT = TestUtils.sampleArtistsDat(CascadingHadoopSuite.configuration);

    @Test
    public void testWriteToES() throws Exception {
        Tap in = sourceTap();
        Tap out = new EsTap("cascading-hadoop/artists", new Fields("name", "url", "picture"));
        Pipe pipe = new Pipe("copy");

        FlowDef flowDef = FlowDef.flowDef().addSource(pipe, in).addTailSink(pipe, out);
        StatsUtils.proxy(new HadoopFlowConnector(HdpBootstrap.asProperties(CascadingHadoopSuite.configuration)).connect(flowDef)).complete();
    }

    @Test
    public void testWriteToESWithAlias() throws Exception {
        Tap in = sourceTap();
        Tap out = new EsTap("cascading-hadoop/alias", "", new Fields("name", "url", "picture"));
        Pipe pipe = new Pipe("copy");

        // rename "id" -> "garbage"
        pipe = new Each(pipe, new Identity(new Fields("garbage", "name", "url", "picture", "ts")));

        Properties props = HdpBootstrap.asProperties(CascadingHadoopSuite.configuration);
        props.setProperty("es.mapping.names", "url:address");
        StatsUtils.proxy(new HadoopFlowConnector(props).connect(in, out, pipe)).complete();
    }

    @Test
    public void testIndexPattern() throws Exception {
        Properties props = HdpBootstrap.asProperties(CascadingHadoopSuite.configuration);

        Tap in = sourceTap();
        Tap out = new EsTap("cascading-hadoop/pattern-{id}", new Fields("id", "name", "url", "picture"));
        Pipe pipe = new Pipe("copy");
        StatsUtils.proxy(new HadoopFlowConnector(props).connect(in, out, pipe)).complete();
    }

    @Test
    public void testIndexPatternWithFormat() throws Exception {
        Properties props = HdpBootstrap.asProperties(CascadingHadoopSuite.configuration);

        Tap in = sourceTap();
        Tap out = new EsTap("cascading-hadoop/pattern-format-{ts:YYYY-MM-dd}", new Fields("id", "name", "url", "picture", "ts"));
        Pipe pipe = new Pipe("copy");
        StatsUtils.proxy(new HadoopFlowConnector(props).connect(in, out, pipe)).complete();
    }

    private Tap sourceTap() {
        return new Hfs(new TextDelimited(new Fields("id", "name", "url", "picture", "ts")), INPUT);
    }
}
