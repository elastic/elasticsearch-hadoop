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
import org.junit.Test;

import cascading.flow.local.LocalFlowConnector;
import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.local.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;

public class CascadingLocalSaveTest {

    private static final String INPUT = "src/test/resources/artists.dat";

    @Test
    public void testWriteToES() throws Exception {
        // local file-system source
        Tap in = new FileTap(new TextDelimited(new Fields("id", "name", "url", "picture")), INPUT);
        Tap out = new EsTap("cascading-local/artists", new Fields("name", "url", "picture"));

        Pipe pipe = new Pipe("copy");
        build(new TestSettings().getProperties(), in, out, pipe);
    }

    @Test
    public void testWriteToESWithAlias() throws Exception {
        // local file-system source
        Tap in = new FileTap(new TextDelimited(new Fields("id", "name", "url", "picture")), INPUT);
        Tap out = new EsTap("cascading-local/alias", new Fields("name", "url", "picture"));
        Pipe pipe = new Pipe("copy");

        // rename "id" -> "garbage"
        pipe = new Each(pipe, new Identity(new Fields("garbage", "name", "url", "picture")));

        Properties props = new TestSettings().getProperties();
        props.setProperty("es.mapping.names", "url:address");
        build(props, in, out, pipe);
    }

    @Test(expected = Exception.class)
    public void testIndexAutoCreateDisabled() throws Exception {
        Properties properties = new TestSettings().getProperties();
        properties.setProperty(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "false");

        // local file-system source
        Tap in = new FileTap(new TextDelimited(new Fields("id", "name", "url", "picture")), INPUT);
        Tap out = new EsTap("cascading-local/non-existing", new Fields("name", "url", "picture"));

        Pipe pipe = new Pipe("copy");

        // rename "id" -> "garbage"
        pipe = new Each(pipe, new Identity(new Fields("garbage", "name", "url", "picture")));
        build(properties, in, out, pipe);
    }

    @Test
    public void testFieldMapping() throws Exception {
        // local file-system source
        Tap in = new FileTap(new TextDelimited(new Fields("id", "name", "url", "picture")), INPUT);
        Tap out = new EsTap("cascading-local/fieldmapping", new Fields("name", "url", "picture"));
        Pipe pipe = new Pipe("copy");

        // rename "id" -> "garbage"
        pipe = new Each(pipe, new Identity(new Fields("garbage", "name", "url", "picture")));

        Properties props = new TestSettings().getProperties();
        props.setProperty("es.mapping.ttl", "<1>");
        build(props, in, out, pipe);
    }

    private void build(Properties cfg, Tap in, Tap out, Pipe pipe) {
        StatsUtils.proxy(new LocalFlowConnector(cfg).connect(in, out, pipe)).complete();
    }
}