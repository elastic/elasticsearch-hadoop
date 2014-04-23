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
import org.elasticsearch.hadoop.util.TestSettings;
import org.elasticsearch.hadoop.util.TestUtils;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import static org.junit.Assert.*;

import static org.hamcrest.CoreMatchers.*;

import cascading.flow.local.LocalFlowConnector;
import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.local.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class AbstractCascadingLocalSaveTest {

    private static final String INPUT = TestUtils.sampleArtistsDat();

    @Test
    public void testWriteToES() throws Exception {
        // local file-system source
        Tap in = sourceTap();
        Tap out = new EsTap("cascading-local/artists", new Fields("name", "url", "picture"));

        Pipe pipe = new Pipe("copy");
        build(new TestSettings().getProperties(), in, out, pipe);
    }

    @Test
    public void testWriteToESMapping() throws Exception {
        assertThat(RestUtils.getMapping("cascading-local/artists").skipHeaders().toString(), is("artists=[name=STRING, picture=STRING, url=STRING]"));
    }

    @Test
    public void testWriteToESWithAlias() throws Exception {
        // local file-system source
        Tap in = sourceTap();
        Tap out = new EsTap("cascading-local/alias", new Fields("name", "url", "picture"));
        Pipe pipe = new Pipe("copy");

        // rename "id" -> "garbage"
        pipe = new Each(pipe, new Identity(new Fields("garbage", "name", "url", "picture", "ts")));

        Properties props = new TestSettings().getProperties();
        props.setProperty("es.mapping.names", "url:address");
        build(props, in, out, pipe);
    }

    @Test
    public void testWriteToESWithAliasMapping() throws Exception {
        assertThat(RestUtils.getMapping("cascading-local/alias").skipHeaders().toString(), is("alias=[address=STRING, name=STRING, picture=STRING]"));
    }


    @Test(expected = Exception.class)
    public void testIndexAutoCreateDisabled() throws Exception {
        Properties properties = new TestSettings().getProperties();
        properties.setProperty(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "false");

        // local file-system source
        Tap in = sourceTap();
        Tap out = new EsTap("cascading-local/non-existing", new Fields("name", "url", "picture"));

        Pipe pipe = new Pipe("copy");

        // rename "id" -> "garbage"
        pipe = new Each(pipe, new Identity(new Fields("garbage", "name", "url", "picture", "ts")));
        build(properties, in, out, pipe);
    }

    @Test
    public void testFieldMapping() throws Exception {
        // local file-system source
        Tap in = sourceTap();
        Tap out = new EsTap("cascading-local/fieldmapping", new Fields("name", "url", "picture"));
        Pipe pipe = new Pipe("copy");

        // rename "id" -> "garbage"
        pipe = new Each(pipe, new Identity(new Fields("garbage", "name", "url", "picture", "ts")));

        Properties props = new TestSettings().getProperties();
        props.setProperty("es.mapping.ttl", "<1>");
        build(props, in, out, pipe);
    }

    @Test
    public void testWriteToESWithtestFieldMappingMapping() throws Exception {
        assertThat(RestUtils.getMapping("cascading-local/fieldmapping").skipHeaders().toString(), is("fieldmapping=[name=STRING, picture=STRING, url=STRING]"));
    }

    @Test
    public void testIndexPattern() throws Exception {
        Properties properties = new TestSettings().getProperties();

        // local file-system source
        Tap in = sourceTap();
        Tap out = new EsTap("cascading-local/pattern-{id}", new Fields("id", "name", "url", "picture"));
        Pipe pipe = new Pipe("copy");
        build(properties, in, out, pipe);
    }

    @Test
    public void testIndexPatternMapping() throws Exception {
        assertThat(RestUtils.getMapping("cascading-local/pattern-12").skipHeaders().toString(), is("pattern-12=[id=STRING, name=STRING, picture=STRING, url=STRING]"));
    }

    @Test
    public void testIndexPatternWithFormatAndAlias() throws Exception {
        Properties properties = new TestSettings().getProperties();

        // local file-system source
        Tap in = sourceTap();
        Tap out = new EsTap("cascading-local/pattern-format-{ts:YYYY-MM-dd}", new Fields("id", "name", "url", "picture", "ts"));
        Pipe pipe = new Pipe("copy");

        build(properties, in, out, pipe);
    }

    @Test
    public void testIndexPatternWithFormatAndAliasMapping() throws Exception {
        assertThat(RestUtils.getMapping("cascading-local/pattern-format-2012-10-06").skipHeaders().toString(), is("pattern-format-2012-10-06=[id=STRING, name=STRING, picture=STRING, ts=DATE, url=STRING]"));
    }


    private Tap sourceTap() {
        return new FileTap(new TextDelimited(new Fields("id", "name", "url", "picture", "ts")), INPUT);
    }

    private void build(Properties cfg, Tap in, Tap out, Pipe pipe) {
        StatsUtils.proxy(new LocalFlowConnector(cfg).connect(in, out, pipe)).complete();
    }
}