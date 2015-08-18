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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.mr.RestUtils;
import org.elasticsearch.hadoop.util.unit.TimeValue;
import org.elasticsearch.storm.EsBolt;
import org.junit.Test;

import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.google.common.collect.ImmutableList;

import static org.junit.Assert.*;

import static org.elasticsearch.integration.storm.AbstractStormSuite.*;
import static org.hamcrest.CoreMatchers.*;

public class AbstractStormIdMappingBoltTests extends AbstractStormBoltTests {

    public AbstractStormIdMappingBoltTests(Map conf, String index) {
        super(conf, index);
    }

    @Test
    public void test2WriteWithId() throws Exception {
        List doc1 = ImmutableList.of("one", "fo1", "two", "fo2", "number", 1);
        List doc2 = ImmutableList.of("OTP", "Otopeni", "SFO", "San Fran", "number", 2);

        Map localCfg = new LinkedHashMap(conf);
        localCfg.put(ConfigurationOptions.ES_MAPPING_ID, "number");

        String target = index + "/id-write";
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("test-spout-2", new TestSpout(ImmutableList.of(doc2, doc1), new Fields("key1", "valo1", "key2",
                "valo2", "key3", "number")));
        builder.setBolt("es-bolt-2", new TestBolt(new EsBolt(target, localCfg))).shuffleGrouping("test-spout-2");

        MultiIndexSpoutStormSuite.run(index + "id-write", builder.createTopology(), COMPONENT_HAS_COMPLETED);

        COMPONENT_HAS_COMPLETED.waitFor(1, TimeValue.timeValueSeconds(10));

        RestUtils.refresh(index);
        Thread.sleep(1000);
        assertTrue(RestUtils.exists(target + "/1"));
        assertTrue(RestUtils.exists(target + "/2"));

        String results = RestUtils.get(target + "/_search?");
        assertThat(results, containsString("two"));
    }
}