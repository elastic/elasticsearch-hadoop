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

import java.io.OutputStream;
import java.util.Collection;
import java.util.Properties;

import org.elasticsearch.hadoop.QueryTestParams;
import org.elasticsearch.hadoop.Stream;
import org.elasticsearch.hadoop.cascading.EsTap;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.mr.RestUtils;
import org.elasticsearch.hadoop.util.TestSettings;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import cascading.flow.local.LocalFlowConnector;
import cascading.operation.AssertionLevel;
import cascading.operation.aggregator.Count;
import cascading.operation.assertion.AssertNotNull;
import cascading.operation.assertion.AssertSizeEquals;
import cascading.operation.assertion.AssertSizeLessThan;
import cascading.operation.filter.FilterNotNull;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.local.TextLine;
import cascading.tap.Tap;
import cascading.tuple.Fields;

@RunWith(Parameterized.class)
public class AbstractCascadingLocalSearchTest {

    @Parameters
    public static Collection<Object[]> queries() {
        return QueryTestParams.localParams();
    }

    private String query;
    private boolean readMetadata;

    public AbstractCascadingLocalSearchTest(String query, boolean readMetadata) {
        this.query = query;
        this.readMetadata = readMetadata;
    }

    private OutputStream OUT = Stream.NULL.stream();

    @Before
    public void before() throws Exception {
        RestUtils.refresh("cascading-local*");
    }

    @Test
    public void testReadFromES() throws Exception {
        Tap in = new EsTap("cascading-local-artists/data", query);
        Pipe pipe = new Pipe("copy");
        pipe = new Each(pipe, new FilterNotNull());
        pipe = new Each(pipe, AssertionLevel.STRICT, new AssertSizeLessThan(5));
        // can't select when using unknown
        //pipe = new Each(pipe, new Fields("name"), AssertionLevel.STRICT, new AssertNotNull());
        pipe = new GroupBy(pipe);
        pipe = new Every(pipe, new Count());

        // print out
        Tap out = new OutputStreamTap(new TextLine(), OUT);
        build(cfg(), in, out, pipe);
    }

    @Test
    public void testReadFromESWithFields() throws Exception {
        Tap in = new EsTap("cascading-local-artists/data", query, new Fields("url", "name"));
        Pipe pipe = new Pipe("copy");
        pipe = new Each(pipe, AssertionLevel.STRICT, new AssertSizeEquals(2));
        pipe = new Each(pipe, AssertionLevel.STRICT, new AssertNotNull());
        pipe = new GroupBy(pipe);
        pipe = new Every(pipe, new Count());

        // print out
        Tap out = new OutputStreamTap(new TextLine(), OUT);
        build(cfg(), in, out, pipe);
    }

    @Test
    public void testReadFromESAliasedField() throws Exception {
        Tap in = new EsTap("cascading-local-alias/data", query, new Fields("address"));
        Pipe pipe = new Pipe("copy");
        pipe = new Each(pipe, AssertionLevel.STRICT, new AssertNotNull());
        pipe = new GroupBy(pipe);
        pipe = new Every(pipe, new Count());

        // print out
        Tap out = new OutputStreamTap(new TextLine(), OUT);
        build(cfg(), in, out, pipe);
    }

    @Test
    public void testReadFromESWithFieldAlias() throws Exception {
        Tap in = new EsTap("cascading-local-alias/data", query, new Fields("url"));
        Pipe pipe = new Pipe("copy");
        pipe = new Each(pipe, AssertionLevel.STRICT, new AssertNotNull());
        pipe = new GroupBy(pipe);
        pipe = new Every(pipe, new Count());

        // print out
        // print out
        Tap out = new OutputStreamTap(new TextLine(), OUT);
        Properties cfg = cfg();
        cfg.setProperty("es.mapping.names", "url:address");
        build(cfg, in, out, pipe);
    }


    @Test
    public void testDynamicPattern() throws Exception {
        Assert.assertTrue(RestUtils.exists("cascading-local-pattern-1/data"));
        Assert.assertTrue(RestUtils.exists("cascading-local-pattern-5/data"));
        Assert.assertTrue(RestUtils.exists("cascading-local-pattern-9/data"));
    }

    @Test
    public void testDynamicPatternFormat() throws Exception {
        Assert.assertTrue(RestUtils.exists("cascading-local-pattern-format-2001-10-06/data"));
        Assert.assertTrue(RestUtils.exists("cascading-local-pattern-format-2005-10-06/data"));
        Assert.assertTrue(RestUtils.exists("cascading-local-pattern-format-2017-10-06/data"));
    }

    private Properties cfg() {
        Properties props = new TestSettings().getProperties();
        props.put(ConfigurationOptions.ES_QUERY, query);
        props.put(ConfigurationOptions.ES_READ_METADATA, readMetadata);
        return props;
    }

    private void build(Properties cfg, Tap in, Tap out, Pipe pipe) {
        StatsUtils.proxy(new LocalFlowConnector(cfg).connect(in, out, pipe)).complete();
    }
}