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
package org.elasticsearch.integration.storm;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.mr.RestUtils;
import org.elasticsearch.hadoop.util.unit.TimeValue;
import org.elasticsearch.storm.EsBolt;
import org.elasticsearch.storm.cfg.StormConfigurationOptions;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import static org.junit.Assert.*;

import static org.elasticsearch.integration.storm.StormSuite.*;
import static org.hamcrest.CoreMatchers.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@RunWith(Parameterized.class)
public class AbstractStormBoltTests {

    private Map conf;
    private String index;

    public AbstractStormBoltTests(Map conf, String index) {
        this.conf = conf;
        this.index = index;
    }


    @Before
    public void setup() {
        // -1 bolt, -1 test
        COMPONENT_HAS_COMPLETED = new Counter(2);
    }

    @After
    public void destroy() {
        COMPONENT_HAS_COMPLETED.decrement();
    }

    @Test
    public void testSimpleWriteTopology() throws Exception {
        List doc1 = Collections.singletonList(ImmutableMap.of("one", 1, "two", 2));
        List doc2 = Collections.singletonList(ImmutableMap.of("OTP", "Otopeni", "SFO", "San Fran"));

        String target = index + "/simple-write";
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("test-spout", new TestSpout(ImmutableList.of(doc1, doc2), new Fields("doc")));
        builder.setBolt("es-bolt", new TestBolt(new EsBolt(target, conf))).shuffleGrouping("test-spout");

        StormSuite.run(index + "simple", builder.createTopology(), COMPONENT_HAS_COMPLETED);

        COMPONENT_HAS_COMPLETED.waitFor(1, TimeValue.timeValueSeconds(10));

        RestUtils.refresh(index);
        assertTrue(RestUtils.exists(target));
        String results = RestUtils.get(target + "/_search?");
        assertThat(results, containsString("SFO"));
    }

    //@Test
    public void test2WriteWithId() throws Exception {
        List doc1 = ImmutableList.of("one", 1, "two", 2, "number", 1);
        List doc2 = ImmutableList.of("OTP", "Otopeni", "SFO", "San Fran", "number", 2);

        Map localCfg = new LinkedHashMap(conf);
        localCfg.put(ConfigurationOptions.ES_MAPPING_ID, "number");

        String target = index + "/id-write";
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("test-spout", new TestSpout(ImmutableList.of(doc1, doc2), new Fields("key1", "val1", "key2", "val2", "key3", "number")));
        builder.setBolt("es-bolt", new TestBolt(new EsBolt(target, localCfg))).shuffleGrouping("test-spout");

        StormSuite.run(index + "id-write", builder.createTopology(), COMPONENT_HAS_COMPLETED);

        COMPONENT_HAS_COMPLETED.waitFor(1, TimeValue.timeValueSeconds(10));

        Thread.sleep(1000);
        RestUtils.refresh(index);
        assertTrue(RestUtils.exists(target + "/1"));
        assertTrue(RestUtils.exists(target + "/2"));

        String results = RestUtils.get(target + "/_search?");
        assertThat(results, containsString("two"));
    }

    @Test
    public void test1WriteIndexPattern() throws Exception {
        List doc1 = ImmutableList.of("one", 1, "two", 2, "number", 1);
        List doc2 = ImmutableList.of("OTP", "Otopeni", "SFO", "San Fran", "number", 2);

        String target = index + "/write-{number}";
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("test-spout", new TestSpout(ImmutableList.of(doc1, doc2), new Fields("key1", "val1", "key2", "val2", "key3", "number")));
        builder.setBolt("es-bolt", new TestBolt(new EsBolt(target, conf))).shuffleGrouping("test-spout");

        StormSuite.run(index + "write-pattern", builder.createTopology(), COMPONENT_HAS_COMPLETED);

        COMPONENT_HAS_COMPLETED.waitFor(1, TimeValue.timeValueSeconds(20));

        Thread.sleep(1000);
        RestUtils.refresh(index);
        assertTrue(RestUtils.exists(index + "/write-1"));
        assertTrue(RestUtils.exists(index + "/write-2"));

        String results = RestUtils.get(index + "/write-1" + "/_search?");
        assertThat(results, containsString("two"));

        results = RestUtils.get(index + "/write-2" + "/_search?");
        assertThat(results, containsString("SFO"));
    }

    @Parameters
    public static Collection<Object[]> configs() throws IOException {
        // no ack
        Map noAck = ImmutableMap.of(StormConfigurationOptions.ES_STORM_BOLT_ACK, Boolean.FALSE.toString());

        // write ack
        Map ack = ImmutableMap.of(StormConfigurationOptions.ES_STORM_BOLT_ACK, Boolean.TRUE.toString());
        return Arrays.asList(new Object[][] { { noAck, "storm-bolt" }, { ack, "storm-bolt-ack" } });
    }
}