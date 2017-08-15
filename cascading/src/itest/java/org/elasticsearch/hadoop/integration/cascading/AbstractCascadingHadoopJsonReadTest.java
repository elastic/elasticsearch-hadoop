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

import cascading.flow.hadoop.HadoopFlowConnector;
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
import com.google.common.collect.Lists;
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

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Collection;
import java.util.Properties;

@RunWith(Parameterized.class)
public class AbstractCascadingHadoopJsonReadTest {

    @Parameters
    public static Collection<Object[]> queries() {
        return QueryTestParams.params();
    }

    private final String indexPrefix = "json-";
    private final String query;
    private final boolean readMetadata;

    public AbstractCascadingHadoopJsonReadTest(String query, boolean readMetadata) {
        this.query = query;
        this.readMetadata = readMetadata;
    }

    @Before
    public void before() throws Exception {
        RestUtils.refresh(indexPrefix + "cascading-hadoop*");
    }

    @Test
    public void testReadFromES() throws Exception {
        Tap in = new EsTap(indexPrefix + "cascading-hadoop-artists/data");
        Pipe pipe = new Pipe("copy");

        Tap out = new HadoopPrintStreamTap(Stream.NULL);
        build(cfg(), in, out, pipe);
    }

    @Test
    public void testReadFromESWithSourceFilter() throws Exception {
//        Tap in = new EsTap(indexPrefix + "cascading-hadoop/artists", new Fields("data"));
        Tap in = new EsTap(indexPrefix + "cascading-hadoop-artists/data");
        Pipe pipe = new Pipe("copy");

        Tap out = new HadoopPrintStreamTap(Stream.NULL);

        Properties cfg = cfg();
        cfg.setProperty("es.read.source.filter", "name");

        build(cfg, in, out, pipe);
    }

    private void build(Properties cfg, Tap in, Tap out, Pipe pipe) {
        StatsUtils.proxy(new HadoopFlowConnector(cfg).connect(in, out, pipe)).complete();
    }

    private Properties cfg() {
        Properties props = HdpBootstrap.asProperties(QueryTestParams.provisionQueries(CascadingHadoopSuite.configuration));
        props.put(ConfigurationOptions.ES_QUERY, query);
        props.put(ConfigurationOptions.ES_READ_METADATA, readMetadata);
        props.put(ConfigurationOptions.ES_OUTPUT_JSON, "true");
        return props;
    }
}