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

import java.util.Map;

import org.elasticsearch.hadoop.mr.RestUtils;
import org.elasticsearch.hadoop.util.unit.TimeValue;
import org.elasticsearch.storm.EsSpout;
import org.junit.Before;
import org.junit.Test;

import backtype.storm.topology.TopologyBuilder;

import static org.junit.Assert.*;

import static org.elasticsearch.integration.storm.AbstractStormSuite.*;

import static org.hamcrest.Matchers.*;

public class AbstractSpoutMultiIndexRead extends AbstractStormSpoutTests {

    private int counter = 0;

    public AbstractSpoutMultiIndexRead(Map conf, String index) {
        super(conf, index);
    }

    @Before
    public void setup() {
        // -1 bolt, -1 test
        COMPONENT_HAS_COMPLETED = new Counter(2);
        CapturingBolt.CAPTURED.clear();
    }

    @Test
    public void testMultiIndexRead() throws Exception {

        counter++;

        RestUtils.postData(index + "/foo",
                "{\"message\" : \"Hello World\",\"message_date\" : \"2014-05-25\"}".getBytes());
        RestUtils.postData(index + "/bar",
                "{\"message\" : \"Goodbye World\",\"message_date\" : \"2014-05-25\"}".getBytes());
        RestUtils.refresh(index);

        String target = "_all/foo";
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("es-spout", new TestSpout(new EsSpout(target)));
        builder.setBolt("test-bolt", new CapturingBolt()).shuffleGrouping("es-spout");

        MultiIndexSpoutStormSuite.run(index + "multi", builder.createTopology(), COMPONENT_HAS_COMPLETED);

        COMPONENT_HAS_COMPLETED.waitFor(1, TimeValue.timeValueSeconds(10));

        String results = RestUtils.get(target + "/_search?");
        assertThat(results, containsString("Hello"));

        assertThat(CapturingBolt.CAPTURED.size(), greaterThanOrEqualTo(counter));
        System.out.println(CapturingBolt.CAPTURED);
    }
}
