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
package org.elasticsearch.storm;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.elasticsearch.hadoop.EsHadoopIllegalStateException;
import org.elasticsearch.hadoop.rest.InitializationUtils;
import org.elasticsearch.hadoop.rest.RestService;
import org.elasticsearch.hadoop.rest.RestService.MultiReaderIterator;
import org.elasticsearch.hadoop.rest.RestService.PartitionDefinition;
import org.elasticsearch.hadoop.serialization.builder.JdkValueReader;
import org.elasticsearch.hadoop.util.StringUtils;
import org.elasticsearch.storm.cfg.StormSettings;
import org.elasticsearch.storm.cfg.TupleFailureHandling;

import static org.elasticsearch.hadoop.cfg.ConfigurationOptions.*;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class EsSpout implements IRichSpout {

    private transient static Log log = LogFactory.getLog(EsSpout.class);

    private Map spoutConfig = new LinkedHashMap();

    private transient SpoutOutputCollector collector;
    private transient MultiReaderIterator iterator;

    private final List<String> tupleFields;

    private boolean ackReads = false;
    private int queueSize = 0;
    private Map<Object, Object> inTransitQueue;
    private Queue<Object[]> replayQueue = null;
    private Map<Object, Integer> retries;
    // keep on trying
    private Integer tupleRetries = Integer.valueOf(-1);
    private TupleFailureHandling tupleFailure = null;

    public EsSpout(String target) {
        this(target, null, null);
    }

    public EsSpout(String target, String query) {
        this(target, query, null);
    }

    public EsSpout(String target, String query, Map configuration) {
        if (configuration != null) {
            spoutConfig.putAll(configuration);
        }
        if (StringUtils.hasText(query)) {
            spoutConfig.put(ES_QUERY, query);
        }
        if (StringUtils.hasText(target)) {
            spoutConfig.put(ES_RESOURCE_READ, target);
        }

        tupleFields = new StormSettings(spoutConfig).getStormSpoutFields();
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;

        LinkedHashMap copy = new LinkedHashMap(conf);
        copy.putAll(spoutConfig);

        StormSettings settings = new StormSettings(copy);

        InitializationUtils.setValueReaderIfNotSet(settings, JdkValueReader.class, log);

        ackReads = settings.getStormSpoutReliable();

        if (ackReads) {
            inTransitQueue = new LinkedHashMap<Object, Object>();
            replayQueue = new LinkedList<Object[]>();
            retries = new HashMap<Object, Integer>();
            queueSize = settings.getStormSpoutReliableQueueSize();
            tupleRetries = settings.getStormSpoutReliableRetriesPerTuple();
            tupleFailure = settings.getStormSpoutReliableTupleFailureHandling();
        }

        int totalTasks = context.getComponentTasks(context.getThisComponentId()).size();
        int currentTask = context.getThisTaskIndex();

        // match the partitions based on the current topology
        List<PartitionDefinition> partitions = RestService.findPartitions(settings, log);
        List<PartitionDefinition> assigned = RestService.assignPartitions(partitions, currentTask, totalTasks);
        iterator = RestService.multiReader(settings, assigned, log);
    }

    @Override
    public void close() {
        if (replayQueue != null) {
            replayQueue.clear();
            replayQueue = null;
        }

        if (retries != null) {
            retries.clear();
            retries = null;
        }

        if (inTransitQueue != null) {
            inTransitQueue.clear();
            inTransitQueue = null;
        }

        if (iterator != null) {
            iterator.close();
        }
    }

    @Override
    public void activate() {
    }

    @Override
    public void deactivate() {
    }

    @Override
    public void nextTuple() {
        // 0 - docId, 1 - doc
        Object[] next = null;

        if (replayQueue != null && !replayQueue.isEmpty()) {
            next = replayQueue.poll();
        }
        else if (iterator.hasNext()) {
            next = iterator.next();
        }


        if (next != null) {
            List<Object> tuple = createTuple(next[1]);

            if (ackReads) {
                if (queueSize > 0) {
                    if (inTransitQueue.size() >= queueSize) {
                        throw new EsHadoopIllegalStateException(String.format("Ack-tuples queue has exceeded the specified size [%s]", inTransitQueue.size()));
                    }
                    inTransitQueue.put(next[0], next[1]);
                }

                collector.emit(tuple, next[0]);
            }
            else {
                collector.emit(tuple);
            }
        }
        else {
            // per doc indication
            try {
                Thread.sleep(1);
            } catch (InterruptedException ex) {
                // interrupted sleep - go on
            }
        }
    }

    private List<Object> createTuple(Object value) {
        List<Object> tuple;
        if (!tupleFields.isEmpty()) {
            tuple = new ArrayList<Object>(tupleFields.size());
            Map<String, Object> doc = (Map<String, Object>) value;

            for (String field : tupleFields) {
                tuple.add(doc.get(field));
            }
            return tuple;
        }

        return Collections.singletonList(value);
    }

    @Override
    public void ack(Object msgId) {
        inTransitQueue.remove(msgId);
        retries.remove(msgId);
    }

    @Override
    public void fail(Object msgId) {
        Object tuple = inTransitQueue.remove(msgId);
        Integer attempts = retries.remove(msgId);
        if (attempts == null) {
            attempts = tupleRetries;
        }

        int primitive = attempts.intValue();
        if (primitive == 0) {
            switch (tupleFailure) {
               case ABORT: throw new EsHadoopIllegalStateException(String.format("Tuple [%s] has failed to be fully processed after [%d] retries; aborting...", tuple, attempts));
               case WARN: log.warn(String.format("Tuple [%s] has failed to be fully processed after [%d] retries; aborting...", tuple, attempts));
               case IGNORE: // move on
            }
            return;
        }
        if (primitive > 0) {
            primitive--;
        }
        // negative means keep on trying

        // retry
        retries.put(msgId, Integer.valueOf(primitive));
        replayQueue.add(new Object[] { msgId, tuple });
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        Fields fields = (this.tupleFields.isEmpty() ? new Fields("doc") : new Fields(this.tupleFields));
        declarer.declare(fields);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
