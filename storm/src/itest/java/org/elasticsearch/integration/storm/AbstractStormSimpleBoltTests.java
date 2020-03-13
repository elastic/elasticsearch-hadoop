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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.storm.shade.com.google.common.collect.ImmutableList;
import org.apache.storm.shade.com.google.common.collect.ImmutableMap;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.elasticsearch.hadoop.rest.RestUtils;
import org.elasticsearch.hadoop.util.TestSettings;
import org.elasticsearch.hadoop.util.unit.TimeValue;
import org.elasticsearch.storm.EsBolt;
import org.junit.Test;

import static org.junit.Assert.*;

import static org.elasticsearch.integration.storm.AbstractStormSuite.COMPONENT_HAS_COMPLETED;
import static org.hamcrest.CoreMatchers.containsString;

public class AbstractStormSimpleBoltTests extends AbstractStormBoltTests {

    public AbstractStormSimpleBoltTests(Map conf, String index) {
        super(conf, index);
        conf.putAll(TestSettings.TESTING_PROPS);
    }

    @Test
    public void testSimpleWriteTopology() throws Exception {
        List doc1 = Collections.singletonList(ImmutableMap.of("one", 1, "two", 2));
        List doc2 = Collections.singletonList(ImmutableMap.of("OTP", "Otopeni", "SFO", "San Fran"));

        String target = index + "/simple-write";
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("test-spout-1", new TestSpout(ImmutableList.of(doc2, doc1), new Fields("doc")));
        builder.setBolt("es-bolt-1", new TestBolt(new EsBolt(target, conf))).shuffleGrouping("test-spout-1");

        MultiIndexSpoutStormSuite.run(index + "simple", builder.createTopology(), COMPONENT_HAS_COMPLETED);

        COMPONENT_HAS_COMPLETED.waitFor(1, TimeValue.timeValueSeconds(10));

        RestUtils.refresh(index);
        assertTrue(RestUtils.exists(target));
        String results = RestUtils.get(target + "/_search?");
        assertThat(results, containsString("SFO"));
    }
}
