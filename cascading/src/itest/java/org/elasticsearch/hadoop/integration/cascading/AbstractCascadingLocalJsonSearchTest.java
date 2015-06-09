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
import org.elasticsearch.hadoop.util.StringUtils;
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
public class AbstractCascadingLocalJsonSearchTest {

    @Parameters
    public static Collection<Object[]> queries() {
        return QueryTestParams.localParams();
    }

    private final String indexPrefix = "json-";
    private final String query;
    private final boolean readMetadata;

    public AbstractCascadingLocalJsonSearchTest(String query, boolean readMetadata) {
        this.query = query;
        this.readMetadata = readMetadata;
    }

    private final OutputStream OUT = Stream.NULL.stream();

    @Before
    public void before() throws Exception {
        RestUtils.refresh(indexPrefix + "cascading-local");
    }


    @Test
    public void testReadFromES() throws Exception {
        Tap in = new EsTap(indexPrefix + "cascading-local/artists");
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
    public void testNestedField() throws Exception {
        String data = "{ \"data\" : { \"map\" : { \"key\" : [ 10, 20 ] } } }";
        RestUtils.postData(indexPrefix + "cascading-local/nestedmap", StringUtils.toUTF(data));

        RestUtils.refresh(indexPrefix + "cascading-local");

        Properties cfg = cfg();
        cfg.setProperty("es.mapping.names", "nested:data.map.key");

        Tap in = new EsTap(indexPrefix + "cascading-local/nestedmap", new Fields("nested"));
        Pipe pipe = new Pipe("copy");
        pipe = new Each(pipe, new FilterNotNull());
        pipe = new Each(pipe, AssertionLevel.STRICT, new AssertSizeLessThan(2));

        // print out
        Tap out = new OutputStreamTap(new TextLine(), OUT);
        build(cfg, in, out, pipe);
    }

    @Test
    public void testDynamicPattern() throws Exception {
        Assert.assertTrue(RestUtils.exists(indexPrefix + "cascading-local/pattern-1"));
        Assert.assertTrue(RestUtils.exists(indexPrefix + "cascading-local/pattern-500"));
        Assert.assertTrue(RestUtils.exists(indexPrefix + "cascading-local/pattern-990"));
    }

    @Test
    public void testDynamicPatternWithFormat() throws Exception {
        Assert.assertTrue(RestUtils.exists(indexPrefix + "cascading-local/pattern-format-2001-10-06"));
        Assert.assertTrue(RestUtils.exists(indexPrefix + "cascading-local/pattern-format-2198-10-06"));
        Assert.assertTrue(RestUtils.exists(indexPrefix + "cascading-local/pattern-format-2890-10-06"));
    }

    private void build(Properties cfg, Tap in, Tap out, Pipe pipe) {
        StatsUtils.proxy(new LocalFlowConnector(cfg).connect(in, out, pipe)).complete();
    }

    private Properties cfg() {
        Properties props = new TestSettings().getProperties();
        props.put(ConfigurationOptions.ES_QUERY, query);
        props.put(ConfigurationOptions.ES_READ_METADATA, readMetadata);
        return props;
    }
}