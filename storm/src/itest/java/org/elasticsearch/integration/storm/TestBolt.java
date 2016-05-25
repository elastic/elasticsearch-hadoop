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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

public class TestBolt implements IRichBolt {

    private static Log log = LogFactory.getLog(TestBolt.class);

    private final IRichBolt delegate;
    private boolean done = false;

    public TestBolt(IRichBolt delegate) {
        this.delegate = delegate;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        delegate.prepare(stormConf, context, collector);
    }

    @Override
    public void execute(Tuple input) {
        // cleanup first to make sure the connection to ES is closed before the test suite shuts down

        if (done) {
            return;
        }

        if (log.isDebugEnabled()) {
            log.debug("Received tuple " + input);
        }
        if (TestSpout.DONE.equals(input.getValue(0))) {
            delegate.cleanup();
            done = true;
            AbstractStormSuite.COMPONENT_HAS_COMPLETED.decrement();
        }
        if (!done) {
            delegate.execute(input);
        }
    }

    public void cleanup() {
        //
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        delegate.declareOutputFields(declarer);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return delegate.getComponentConfiguration();
    }
}
