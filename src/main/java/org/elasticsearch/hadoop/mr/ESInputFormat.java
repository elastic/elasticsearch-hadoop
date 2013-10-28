/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticsearch.hadoop.mr;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.*;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.cfg.SettingsManager;
import org.elasticsearch.hadoop.rest.BufferedRestClient;
import org.elasticsearch.hadoop.rest.QueryBuilder;
import org.elasticsearch.hadoop.rest.ScrollQuery;
import org.elasticsearch.hadoop.rest.dto.Node;
import org.elasticsearch.hadoop.rest.dto.Shard;
import org.elasticsearch.hadoop.rest.dto.mapping.Field;
import org.elasticsearch.hadoop.serialization.ScrollReader;
import org.elasticsearch.hadoop.serialization.SerializationUtils;
import org.elasticsearch.hadoop.serialization.ValueReader;
import org.elasticsearch.hadoop.util.IOUtils;
import org.elasticsearch.hadoop.util.ObjectUtils;
import org.elasticsearch.hadoop.util.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * ElasticSearch {@link InputFormat} for streaming data (typically based on a query) from ElasticSearch.
 * Returns the document ID as key and its content as value.
 *
 * <p/>This class implements both the "old" (<tt>org.apache.hadoop.mapred</tt>) and the "new" (<tt>org.apache.hadoop.mapreduce</tt>) API.
 */
public class ESInputFormat<K, V> extends InputFormat<K, V> implements org.apache.hadoop.mapred.InputFormat<K, V>,
        ConfigurationOptions {

    private static Log log = LogFactory.getLog(ESInputFormat.class);

    protected static class ShardInputSplit extends InputSplit implements org.apache.hadoop.mapred.InputSplit {

        private String nodeIp;
        private int httpPort;
        private String nodeId;
        private String nodeName;
        private String shardId;
        private String mapping;

        public ShardInputSplit() {}

        public ShardInputSplit(String nodeIp, int httpPort, String nodeId, String nodeName, Integer shard, String mapping) {
            this.nodeIp = nodeIp;
            this.httpPort = httpPort;
            this.nodeId = nodeId;
            this.nodeName = nodeName;
            this.shardId = shard.toString();
            this.mapping = mapping;
        }

        @Override
        public long getLength() {
            // TODO: can this be computed easily?
            return 1l;
        }

        @Override
        public String[] getLocations() {
            // TODO: check whether the host name needs to be used instead
            return new String[] { nodeIp };
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(nodeIp);
            out.writeInt(httpPort);
            out.writeUTF(nodeId);
            out.writeUTF(nodeName);
            out.writeUTF(shardId);
            out.writeUTF(mapping);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            nodeIp = in.readUTF();
            httpPort = in.readInt();
            nodeId = in.readUTF();
            nodeName = in.readUTF();
            shardId = in.readUTF();
            mapping = in.readUTF();
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("ShardInputSplit [node=[").append(nodeId).append("/").append(nodeName)
                        .append("|").append(nodeIp).append(":").append(httpPort)
                        .append("],shard=").append(shardId).append("]");
            return builder.toString();
        }

    }


    protected static abstract class ShardRecordReader<K,V> extends RecordReader<K, V> implements
            org.apache.hadoop.mapred.RecordReader<K, V> {

        private int read = 0;
        private ShardInputSplit esSplit;
        private ScrollReader scrollReader;

        private BufferedRestClient client;
        private QueryBuilder queryBuilder;
        private ScrollQuery result;

        // reuse objects
        private K currentKey;
        private V currentValue;

        private long size = 0;

        // default constructor used by the NEW api
        public ShardRecordReader() {
        }

        // constructor used by the old API
        public ShardRecordReader(org.apache.hadoop.mapred.InputSplit split, Configuration job, Reporter reporter) {
            reporter.setStatus(split.toString());
            init((ShardInputSplit) split, job);
        }

        // new API init call
        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
            context.setStatus(split.toString());
            init((ShardInputSplit) split, context.getConfiguration());
        }

        void init(ShardInputSplit esSplit, Configuration cfg) {
            Settings settings = SettingsManager.loadFrom(cfg);

            // override the global settings to communicate directly with the target node
            settings.cleanUri().setHost(esSplit.nodeIp).setPort(esSplit.httpPort);

            this.esSplit = esSplit;

            // initialize mapping/ scroll reader
            SerializationUtils.setValueReaderIfNotSet(settings, WritableValueReader.class, log);
            ValueReader reader = ObjectUtils.instantiate(settings.getSerializerValueReaderClassName(), settings);

            String mappingData = esSplit.mapping;

            Field mapping = null;

            if (StringUtils.hasText(mappingData)) {
                mapping = IOUtils.deserializeFromBase64(mappingData);
            }
            else {
                log.warn(String.format("No mapping found for [%s] - either no index exists or the split configuration has been corrupted", esSplit));
            }

            scrollReader = new ScrollReader(reader, mapping);

            // initialize REST client
            client = new BufferedRestClient(settings);

            queryBuilder = QueryBuilder.query(settings)
                    .shard(esSplit.shardId)
                    .onlyNode(esSplit.nodeId);

            if (log.isDebugEnabled()) {
                log.debug(String.format("Initializing RecordReader for [%s]", esSplit));
            }
        }

        @Override
        public boolean nextKeyValue() throws IOException {
            // new API call routed to old API
            if (currentKey == null) {
                currentKey = createKey();
            }
            if (currentValue == null) {
                currentValue = createValue();
            }

            // FIXME: does the new API mandate a new instance each time (?)
            return next(currentKey, currentValue);
        }

        @Override
        public K getCurrentKey() throws IOException {
            return currentKey;
        }

        @Override
        public V getCurrentValue() {
            return currentValue;
        }

        @Override
        public float getProgress() {
            return size == 0 ? 0 : ((float) getPos()) / size;
        }

        @Override
        public void close() throws IOException {
            if (log.isDebugEnabled()) {
                log.debug(String.format("Closing RecordReader for [%s]", esSplit));
            }

            if (result != null) {
                result.close();
                result = null;
            }
            client.close();
        }

        @Override
        public boolean next(K key, V value) throws IOException {
            if (result == null) {
                result = queryBuilder.build(client, scrollReader);
                size = result.getSize();

                if (log.isTraceEnabled()) {
                    log.trace(String.format("Received scroll [%s],  size [%d] for query [%s]", result, size, queryBuilder));
                }
            }

            boolean hasNext = result.hasNext();

            if (!hasNext) {
                return false;
            }

            Object[] next = result.next();
            currentKey = setCurrentKey(currentKey, key, next[0]);
            currentValue = setCurrentValue(currentValue, value, next[1]);

            // keep on counting
            read++;
            return true;
        }

        @Override
        public abstract K createKey();

        @Override
        public abstract V createValue();

        protected abstract K setCurrentKey(K oldApiKey, K newApiKey, Object object);

        protected abstract V setCurrentValue(V oldApiValue, V newApiKey, Object object);

        @Override
        public long getPos() {
            return read;
        }
    }

    protected static class WritableShardRecordReader extends ShardRecordReader<Text, MapWritable> {
        public WritableShardRecordReader() {
            super();
        }

        public WritableShardRecordReader(org.apache.hadoop.mapred.InputSplit split, Configuration job, Reporter reporter) {
            super(split, job, reporter);
        }

        @Override
        public Text createKey() {
            return new Text();
        }

        @Override
        public MapWritable createValue() {
            return new MapWritable();
        }

        @Override
        protected Text setCurrentKey(Text oldApiKey, Text newApiKey, Object object) {
            String val = object.toString();
            if (oldApiKey == null) {
                oldApiKey = new Text();
                oldApiKey.set(val);
            }

            // new API might not be used
            if (newApiKey != null) {
                newApiKey.set(val);
            }
            return oldApiKey;
        }

        @Override
        protected MapWritable setCurrentValue(MapWritable oldApiValue, MapWritable newApiKey, Object object) {
            MapWritable val = (MapWritable) object;
            if (newApiKey != null) {
                newApiKey.clear();
                newApiKey.putAll(val);
            }
            return val;
        }
    }

    //
    // new API - just delegates to the Old API
    //
    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException {
        JobConf conf = (JobConf) context.getConfiguration();
        // NOTE: this method expects a ShardInputSplit to be returned (which implements both the old and the new API).
        return Arrays.asList((InputSplit[]) getSplits(conf, conf.getNumMapTasks()));
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public ShardRecordReader<K, V> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return (ShardRecordReader<K, V>) new WritableShardRecordReader();
    }


    //
    // Old API - if this method is replaced, make sure to return a new/old-API compatible InputSplit
    //

    // Note: data written to the JobConf will be silently discarded
    @Override
    public org.apache.hadoop.mapred.InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {

        Settings settings = SettingsManager.loadFrom(job);
        BufferedRestClient client = new BufferedRestClient(settings);
        Map<Shard, Node> targetShards = client.getTargetShards();

        String savedMapping = null;
        if (!targetShards.isEmpty()) {
            Field mapping = client.getMapping();
            //TODO: implement this more efficiently
            savedMapping = IOUtils.serializeToBase64(mapping);
            log.info(String.format("Discovered mapping {%s} for [%s]", mapping, settings.getTargetResource()));
        }

        client.close();

        if (settings.getIndexReadMissingAsEmpty() && targetShards.isEmpty()) {
            log.info(String.format("Index [%s] missing - treating it as empty", settings.getTargetResource()));
        }

        else if (log.isTraceEnabled()) {
            log.trace("Creating splits for shards " + targetShards);
        }

        ShardInputSplit[] splits = new ShardInputSplit[targetShards.size()];

        int index = 0;
        for (Entry<Shard, Node> entry : targetShards.entrySet()) {
            Shard shard = entry.getKey();
            Node node = entry.getValue();
            splits[index++] =
                        new ShardInputSplit(node.getIpAddress(), node.getHttpPort(), node.getId(), node.getName(), shard.getName(), savedMapping);
        }

        log.info(String.format("Created [%d] shard-splits", splits.length));
        return splits;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public ShardRecordReader<K, V> getRecordReader(org.apache.hadoop.mapred.InputSplit split, JobConf job, Reporter reporter) {
        return (ShardRecordReader<K, V>) new WritableShardRecordReader(split, job, reporter);
    }
}