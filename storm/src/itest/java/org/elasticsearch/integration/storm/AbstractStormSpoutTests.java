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
import java.util.Map;

import org.elasticsearch.hadoop.mr.RestUtils;
import org.elasticsearch.hadoop.util.unit.TimeValue;
import org.elasticsearch.storm.EsSpout;
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

import com.google.common.collect.ImmutableMap;

import static org.junit.Assert.*;

import static org.elasticsearch.integration.storm.SpoutStormSuite.*;
import static org.hamcrest.CoreMatchers.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@RunWith(Parameterized.class)
public class AbstractStormSpoutTests {

    private Map conf;
    private String index;

    public AbstractStormSpoutTests(Map conf, String index) {
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
    public void testSimpleRead() throws Exception {
        String target = index + "/basic-read";

        RestUtils.touch(index);
        RestUtils.putData(target, "{\"message\" : \"Hello World\",\"message_date\" : \"2014-05-25\"}".getBytes());
        RestUtils.putData(target, "{\"message\" : \"Goodbye World\",\"message_date\" : \"2014-05-25\"}".getBytes());
        RestUtils.refresh(index);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("es-spout", new TestSpout(new EsSpout(target)));
        builder.setBolt("test-bolt", new CapturingBolt()).shuffleGrouping("es-spout");

        SpoutStormSuite.run(index + "simple", builder.createTopology(), COMPONENT_HAS_COMPLETED);

        COMPONENT_HAS_COMPLETED.waitFor(1, TimeValue.timeValueSeconds(10));

        assertTrue(RestUtils.exists(target));
        String results = RestUtils.get(target + "/_search?");
        assertThat(results, containsString("Hello"));
        assertThat(results, containsString("Goodbye"));
    }

    @Test
    public void testMultiIndexRead() throws Exception {

        RestUtils.putData(index + "/foo", "{\"message\" : \"Hello World\",\"message_date\" : \"2014-05-25\"}".getBytes());
        RestUtils.putData(index + "/bar", "{\"message\" : \"Goodbye World\",\"message_date\" : \"2014-05-25\"}".getBytes());
        RestUtils.refresh(index);

        String target = "_all/foo";
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("es-spout", new TestSpout(new EsSpout(target)));
        builder.setBolt("test-bolt", new CapturingBolt()).shuffleGrouping("es-spout");

        SpoutStormSuite.run(index + "multi", builder.createTopology(), COMPONENT_HAS_COMPLETED);

        COMPONENT_HAS_COMPLETED.waitFor(1, TimeValue.timeValueSeconds(10));

        String results = RestUtils.get(target + "/_search?");
        assertThat(results, containsString("Hello"));
    }

    @Parameters
    public static Collection<Object[]> configs() throws IOException {
        // no ack
        Map noAck = ImmutableMap.of(StormConfigurationOptions.ES_STORM_SPOUT_RELIABLE, Boolean.FALSE.toString());

        // read ack
        Map ack = ImmutableMap.of(StormConfigurationOptions.ES_STORM_SPOUT_RELIABLE, Boolean.TRUE.toString());

        // read ack bounded queue
        Map ackWithSize = ImmutableMap.of(StormConfigurationOptions.ES_STORM_SPOUT_RELIABLE, Boolean.TRUE.toString(), StormConfigurationOptions.ES_STORM_SPOUT_RELIABLE_QUEUE_SIZE, "1");

        return Arrays.asList(new Object[][] {
                { noAck, "storm-spout" },
                { ack, "storm-spout-reliable" },
                { ackWithSize, "storm-spout-reliable-size" } });
    }

}