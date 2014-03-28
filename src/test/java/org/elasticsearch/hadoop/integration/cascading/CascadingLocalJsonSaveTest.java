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
import cascading.pipe.Pipe;
import cascading.scheme.local.TextLine;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;

public class CascadingLocalJsonSaveTest {

    private static final String INPUT = "src/test/resources/artists.json";

    @Test
    public void testWriteToES() throws Exception {
        // local file-system source
        Tap in = new FileTap(new TextLine(new Fields("line")), INPUT);
        Tap out = new EsTap("json-cascading-local/artists");

        Pipe pipe = new Pipe("copy");
        Properties props = new TestSettings().getProperties();
        props.setProperty(ConfigurationOptions.ES_SERIALIZATION_WRITER_BYTES_CLASS, "true");
        build(props, in, out, pipe);
    }

    @Test(expected = Exception.class)
    public void testIndexAutoCreateDisabled() throws Exception {
        Properties properties = new TestSettings().getProperties();
        properties.setProperty(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "false");
        properties.setProperty(ConfigurationOptions.ES_SERIALIZATION_WRITER_BYTES_CLASS, "true");

        // local file-system source
        Tap in = new FileTap(new TextLine(new Fields("line")), INPUT);
        Tap out = new EsTap("json-cascading-local/non-existing", new Fields("line"));
        Pipe pipe = new Pipe("copy");
        build(properties, in, out, pipe);
    }

    private void build(Properties props, Tap in, Tap out, Pipe pipe) {
        StatsUtils.proxy(new LocalFlowConnector(props).connect(in, out, pipe)).complete();
    }
}