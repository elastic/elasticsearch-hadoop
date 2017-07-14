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

import java.util.Collection;
import java.util.Properties;

import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.HdpBootstrap;
import org.elasticsearch.hadoop.QueryTestParams;
import org.elasticsearch.hadoop.Stream;
import org.elasticsearch.hadoop.cascading.EsTap;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.mr.RestUtils;
import org.elasticsearch.hadoop.util.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.AssertionLevel;
import cascading.operation.assertion.AssertNotNull;
import cascading.operation.assertion.AssertSizeEquals;
import cascading.operation.assertion.AssertSizeLessThan;
import cascading.operation.filter.FilterNotNull;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.tuple.Fields;

@RunWith(Parameterized.class)
public class AbstractCascadingHadoopSearchTest {

    @Parameters
    public static Collection<Object[]> queries() {
        return QueryTestParams.params();
    }

    private final String query;
    private final boolean readMetadata;

    public AbstractCascadingHadoopSearchTest(String query, boolean readMetadata) {
        this.query = query;
        this.readMetadata = readMetadata;
    }

    @Before
    public void before() throws Exception {
        RestUtils.refresh("cascading-hadoop*");
    }

    @Test
    public void testReadFromES() throws Exception {
        Tap in = new EsTap("cascading-hadoop-artists/data", query);
        Pipe pipe = new Pipe("copy");
        pipe = new Each(pipe, AssertionLevel.STRICT, new AssertSizeLessThan(5));
        pipe = new Each(pipe, AssertionLevel.STRICT, new AssertNotNull());

        // print out
        Tap out = new HadoopPrintStreamTap(Stream.NULL);
        //Tap out = new Hfs(new TextDelimited(), "cascadingbug-1", SinkMode.REPLACE);
        //FlowDef flowDef = FlowDef.flowDef().addSource(pipe, in).addTailSink(pipe, out);

        build(cfg(), in, out, pipe);
    }


    @Test
    public void testReadFromESWithFields() throws Exception {
        Tap in = new EsTap("cascading-hadoop-artists/data", query, new Fields("url", "name"));
        Pipe pipe = new Pipe("copy");
        pipe = new Each(pipe, AssertionLevel.STRICT, new AssertSizeEquals(2));
        pipe = new Each(pipe, AssertionLevel.STRICT, new AssertNotNull());

        // print out
        Tap out = new HadoopPrintStreamTap(Stream.NULL);
        build(cfg(), in, out, pipe);
    }

    @Test
    public void testReadFromESAliasedField() throws Exception {
        Tap in = new EsTap("cascading-hadoop-alias/data", query, new Fields("address"));
        Pipe pipe = new Pipe("copy");
        pipe = new Each(pipe, AssertionLevel.STRICT, new AssertNotNull());

        // print out
        Tap out = new HadoopPrintStreamTap(Stream.NULL);
        build(cfg(), in, out, pipe);
    }

    @Test
    public void testReadFromESWithFieldAlias() throws Exception {
        Tap in = new EsTap("cascading-hadoop-alias/data", query, new Fields("url"));
        Pipe pipe = new Pipe("copy");
        pipe = new Each(pipe, AssertionLevel.STRICT, new AssertNotNull());

        // print out
        Tap out = new HadoopPrintStreamTap(Stream.NULL);
        Properties cfg = cfg();
        cfg.setProperty("es.mapping.names", "url:address");
        build(cfg, in, out, pipe);
    }

    @Test
    public void testNestedField() throws Exception {
        String data = "{ \"data\" : { \"map\" : { \"key\" : [ 10, 20 ] } } }";
        RestUtils.postData("cascading-hadoop-nestedmap/data", StringUtils.toUTF(data));
        RestUtils.refresh("cascading-hadoop*");

        Properties cfg = cfg();
        cfg.setProperty("es.mapping.names", "nested:data.map.key");

        Tap in = new EsTap("cascading-hadoop-nestedmap/data", new Fields("nested"));
        Pipe pipe = new Pipe("copy");
        pipe = new Each(pipe, new FilterNotNull());
        pipe = new Each(pipe, AssertionLevel.STRICT, new AssertSizeLessThan(2));

        // print out
        Tap out = new HadoopPrintStreamTap(Stream.NULL);
        build(cfg, in, out, pipe);
    }

    @Test
    public void testReadFromESWithSourceFiltering() throws Exception {
        Tap in = new EsTap("cascading-hadoop-artists/data", query);
        Pipe pipe = new Pipe("copy");
        pipe = new Each(pipe, AssertionLevel.STRICT, new AssertSizeLessThan(5));
        pipe = new Each(pipe, AssertionLevel.STRICT, new AssertNotNull());

        // print out
        Tap out = new HadoopPrintStreamTap(Stream.NULL);

        Properties cfg = cfg();
        cfg.setProperty("es.read.source.filter", "name");

        build(cfg, in, out, pipe);
    }

    @Test(expected = EsHadoopIllegalArgumentException.class)
    public void testReadFromESWithSourceFilteringCollision() throws Exception {
        Tap in = new EsTap("cascading-hadoop-artists/data", query, new Fields("url", "name"));
        Pipe pipe = new Pipe("copy");
        pipe = new Each(pipe, AssertionLevel.STRICT, new AssertSizeEquals(2));
        pipe = new Each(pipe, AssertionLevel.STRICT, new AssertNotNull());

        // print out
        Tap out = new HadoopPrintStreamTap(Stream.NULL);

        Properties cfg = cfg();
        cfg.setProperty("es.read.source.filter", "name");

        build(cfg, in, out, pipe);
    }

    @Test
    public void testDynamicPattern() throws Exception {
        Assert.assertTrue(RestUtils.exists("cascading-hadoop-pattern-1/data"));
        Assert.assertTrue(RestUtils.exists("cascading-hadoop-pattern-5/data"));
        Assert.assertTrue(RestUtils.exists("cascading-hadoop-pattern-9/data"));
    }

    @Test
    public void testDynamicPatternFormat() throws Exception {
        Assert.assertTrue(RestUtils.exists("cascading-hadoop-pattern-format-2001-10-06/data"));
        Assert.assertTrue(RestUtils.exists("cascading-hadoop-pattern-format-2005-10-06/data"));
        Assert.assertTrue(RestUtils.exists("cascading-hadoop-pattern-format-2017-10-06/data"));
    }

    private Properties cfg() {
        Properties props = HdpBootstrap.asProperties(QueryTestParams.provisionQueries(CascadingHadoopSuite.configuration));
        props.put(ConfigurationOptions.ES_QUERY, query);
        props.put(ConfigurationOptions.ES_READ_METADATA, readMetadata);

        return props;
    }

    private void build(Properties cfg, Tap in, Tap out, Pipe pipe) {
        StatsUtils.proxy(new HadoopFlowConnector(cfg).connect(in, out, pipe)).complete();
    }
}