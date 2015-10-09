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
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.hadoop.util.TestSettings;
import org.elasticsearch.storm.cfg.StormConfigurationOptions;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.ImmutableMap;

import static org.elasticsearch.integration.storm.AbstractStormSuite.COMPONENT_HAS_COMPLETED;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@RunWith(Parameterized.class)
public abstract class AbstractStormBoltTests {

    protected Map conf;
    protected String index;

    public AbstractStormBoltTests(Map conf, String index) {
        this.conf = conf;
        this.index = index;
        conf.putAll(TestSettings.TESTING_PROPS);
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

    @Parameters
    public static Collection<Object[]> configs() throws IOException {
        // no ack
        Map noAck = new HashMap(ImmutableMap.of(StormConfigurationOptions.ES_STORM_BOLT_ACK, Boolean.FALSE.toString()));

        // write ack
        Map ack = new HashMap(ImmutableMap.of(StormConfigurationOptions.ES_STORM_BOLT_ACK, Boolean.TRUE.toString()));
        return Arrays.asList(new Object[][] { { noAck, "storm-bolt" }, { ack, "storm-bolt-ack" } });
    }
}