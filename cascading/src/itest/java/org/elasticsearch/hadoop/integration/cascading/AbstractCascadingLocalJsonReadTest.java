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

import cascading.flow.local.LocalFlowConnector;
import cascading.pipe.Pipe;
import cascading.scheme.local.TextLine;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import org.elasticsearch.hadoop.QueryTestParams;
import org.elasticsearch.hadoop.cascading.EsTap;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.mr.RestUtils;
import org.elasticsearch.hadoop.util.TestSettings;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import static org.hamcrest.Matchers.hasItems;
import static org.junit.Assert.assertThat;

@RunWith(Parameterized.class)
public class AbstractCascadingLocalJsonReadTest {

    @Parameters
    public static Collection<Object[]> queries() {
        return QueryTestParams.localParams();
    }

    private final String indexPrefix = "json-";
    private final String query;
    private final boolean readMetadata;

    public AbstractCascadingLocalJsonReadTest(String query, boolean readMetadata) {
        this.query = query;
        this.readMetadata = readMetadata;
    }

    @Before
    public void before() throws Exception {
        RestUtils.refresh(indexPrefix + "cascading-local*");
    }

    @Test
    public void testReadFromES() throws Exception {
        Tap in = new EsTap(indexPrefix + "cascading-local-artists/data");
        Pipe pipe = new Pipe("copy");
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        Tap out = new OutputStreamTap(new TextLine(), os);
        build(cfg(), in, out, pipe);

        BufferedReader r = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(os.toByteArray())));

        List<String> records = new ArrayList<>();
        for (String line = r.readLine(); line != null; line = r.readLine()) {
            records.add(line);
        }

        String doc1 = "{\"number\":\"917\",\"name\":\"Iron Maiden\",\"url\":\"http://www.last.fm/music/Iron+Maiden\",\"picture\":\"http://userserve-ak.last.fm/serve/252/22493569.jpg\",\"@timestamp\":\"2017-10-06T19:20:25.000Z\",\"list\":[\"quick\", \"brown\", \"fox\"],\"tag\":\"9\"}";
        String doc2 = "{\"number\":\"979\",\"name\":\"Smash Mouth\",\"url\":\"http://www.last.fm/music/Smash+Mouth\",\"picture\":\"http://userserve-ak.last.fm/serve/252/82063.jpg\",\"@timestamp\":\"2017-10-06T19:20:25.000Z\",\"list\":[\"quick\", \"brown\", \"fox\"],\"tag\":\"9\"}";
        String doc3 = "{\"number\":\"190\",\"name\":\"Muse\",\"url\":\"http://www.last.fm/music/Muse\",\"picture\":\"http://userserve-ak.last.fm/serve/252/416514.jpg\",\"@timestamp\":\"2005-10-06T19:20:25.000Z\",\"list\":[\"quick\", \"brown\", \"fox\"],\"tag\":\"1\"}";

        assertThat(records, hasItems(doc1, doc2, doc3));
    }

    @Test
    public void testReadFromESWithSourceFilter() throws Exception {
        Tap in = new EsTap(indexPrefix + "cascading-local-artists/data");
        Pipe pipe = new Pipe("copy");
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        Tap out = new OutputStreamTap(new TextLine(), os);

        Properties cfg = cfg();
        cfg.setProperty("es.read.source.filter", "name");

        build(cfg, in, out, pipe);

        BufferedReader r = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(os.toByteArray())));

        List<String> records = new ArrayList<>();
        for (String line = r.readLine(); line != null; line = r.readLine()) {
            records.add(line);
        }

        String doc1 = "{\"name\":\"Iron Maiden\"}";
        String doc2 = "{\"name\":\"Smash Mouth\"}";
        String doc3 = "{\"name\":\"Muse\"}";

        assertThat(records, hasItems(doc1, doc2, doc3));
    }

    private void build(Properties cfg, Tap in, Tap out, Pipe pipe) {
        StatsUtils.proxy(new LocalFlowConnector(cfg).connect(in, out, pipe)).complete();
    }

    private Properties cfg() {
        Properties props = new TestSettings().getProperties();
        props.put(ConfigurationOptions.ES_QUERY, query);
        props.put(ConfigurationOptions.ES_READ_METADATA, readMetadata);
        props.put(ConfigurationOptions.ES_OUTPUT_JSON, "true");
        return props;
    }
}