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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;

public class TestSpout extends BaseRichSpout {

    class InterceptingSpoutOutputCollector extends SpoutOutputCollector {
        boolean emitted = false;

        public InterceptingSpoutOutputCollector(SpoutOutputCollector collector) {
            super(collector);
        }

        @Override
        public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
            emitted = true;
            return super.emit(streamId, tuple, messageId);
        }

        public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId) {
            emitted = true;
            super.emitDirect(taskId, streamId, tuple, messageId);
        }


        public boolean hasEmitted() {
            return emitted;
        }

        public void reset() {
            emitted = false;
        }
    }

    private InterceptingSpoutOutputCollector collector;
    private List<List> tuples;
    private Fields fields;
    private boolean done = false;

    private final IRichSpout spout;

    private static Log log = LogFactory.getLog(TestSpout.class);

    public static final Object DONE = "TestSpout-Done";
    public List<Object> DONE_TUPLE = null;

    public TestSpout(IRichSpout delegate) {
        this.spout = delegate;
        DONE_TUPLE = Collections.singletonList(DONE);
    }

    public TestSpout(List<List> tuples, Fields output) {
        this.tuples = tuples;
        this.fields = output;
        this.spout = null;
        DONE_TUPLE = new ArrayList(output.size());
        for (int i = 0; i < output.size(); i++) {
            DONE_TUPLE.add(DONE);
        }
    }

    public TestSpout(List<List> tuples, Fields output, boolean noDoneTuple) {
        this.tuples = tuples;
        this.fields = output;
        this.spout = null;
        if (noDoneTuple) {
            DONE_TUPLE = null;
        }
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = new InterceptingSpoutOutputCollector(collector);

        if (spout != null) {
            spout.open(conf, context, this.collector);
        }
        log.info("Opened TestSpout");
    }

    @Override
    public void nextTuple() {
        if (done)
            return;

        collector.reset();

        if (spout != null) {
            spout.nextTuple();

            if (!collector.hasEmitted()) {
                done = true;
            }
        }
        else {
            for (List tuple : tuples) {
                log.info("Emitting tuple...");
                collector.emit(tuple);
            }
            done = true;
        }

        if (done && DONE_TUPLE != null) {
            log.info("Spout finished emitting; sending signal");
            // done
            collector.emit(DONE_TUPLE);
        } else {
            log.info("Spout finished emitting");
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        if (spout != null) {
            spout.declareOutputFields(declarer);
        }
        else {
            declarer.declare(fields);
        }
    }

    public void close() {
        if (spout != null)
            spout.close();
    }

    public void activate() {
        if (spout != null)
            spout.activate();
    }

    public void deactivate() {
        if (spout != null)
            spout.deactivate();
    }

    public void ack(Object msgId) {
        if (spout != null)
            spout.ack(msgId);
    }

    public void fail(Object msgId) {
        if (spout != null)
            spout.fail(msgId);
    }
}
