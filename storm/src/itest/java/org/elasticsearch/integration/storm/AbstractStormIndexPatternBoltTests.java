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

import java.util.List;
import java.util.Map;

import org.apache.storm.shade.com.google.common.collect.ImmutableList;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.elasticsearch.hadoop.mr.RestUtils;
import org.elasticsearch.hadoop.util.unit.TimeValue;
import org.elasticsearch.storm.EsBolt;
import org.junit.Test;

import static org.junit.Assert.*;

import static org.elasticsearch.integration.storm.AbstractStormSuite.COMPONENT_HAS_COMPLETED;
import static org.hamcrest.CoreMatchers.containsString;

public class AbstractStormIndexPatternBoltTests extends AbstractStormBoltTests {

    public AbstractStormIndexPatternBoltTests(Map conf, String index) {
        super(conf, index);
    }

    @Test
    public void test1WriteIndexPattern() throws Exception {
        List doc1 = ImmutableList.of("one", "1", "two", "2", "number", 1);
        List doc2 = ImmutableList.of("OTP", "Otopeni", "SFO", "San Fran", "number", 2);

        String target = index + "/write-{number}";
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("test-spout-3", new TestSpout(ImmutableList.of(doc2, doc1), new Fields("key1", "val1", "key2",
                "val2", "key3", "number")));
        builder.setBolt("es-bolt-3", new TestBolt(new EsBolt(target, conf))).shuffleGrouping("test-spout-3");

        MultiIndexSpoutStormSuite.run(index + "write-pattern", builder.createTopology(), COMPONENT_HAS_COMPLETED);

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
}
