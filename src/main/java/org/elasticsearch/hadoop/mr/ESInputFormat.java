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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.cfg.SettingsManager;
import org.elasticsearch.hadoop.rest.BufferedRestClient;
import org.elasticsearch.hadoop.rest.Node;
import org.elasticsearch.hadoop.rest.QueryBuilder;
import org.elasticsearch.hadoop.rest.ScrollQuery;
import org.elasticsearch.hadoop.rest.Shard;
import org.elasticsearch.hadoop.util.WritableUtils;

/**
 * ElasticSearch {@link InputFormat} for streaming data (typically based on a query) from ElasticSearch.
 * Returns the document ID as key and its content as value.
 *
 * <p/>This class implements both the "old" (<tt>org.apache.hadoop.mapred</tt>) and the "new" (<tt>org.apache.hadoop.mapreduce</tt>) API.
 */
public class ESInputFormat extends InputFormat<Text, MapWritable> implements
        org.apache.hadoop.mapred.InputFormat<Text, MapWritable>, ConfigurationOptions {


    private static Log log = LogFactory.getLog(ESInputFormat.class);

    static class ShardInputSplit extends InputSplit implements org.apache.hadoop.mapred.InputSplit {

        private String nodeIp;
        private int httpPort;
        private String nodeId;
        private String nodeName;
        private String shardId;

        public ShardInputSplit() {}

        public ShardInputSplit(String nodeIp, int httpPort, String nodeId, String nodeName, Integer shard) {
            this.nodeIp = nodeIp;
            this.httpPort = httpPort;
            this.nodeId = nodeId;
            this.nodeName = nodeName;
            this.shardId = shard.toString();
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
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            nodeIp = in.readUTF();
            httpPort = in.readInt();
            nodeId = in.readUTF();
            nodeName = in.readUTF();
            shardId = in.readUTF();
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


    static class ShardRecordReader extends RecordReader<Text, MapWritable> implements
            org.apache.hadoop.mapred.RecordReader<Text, MapWritable> {

        private int read = 0;

        private BufferedRestClient client;
        private QueryBuilder queryBuilder;
        private ScrollQuery result;

        // minor optimization - see below
        private String currentKey;
        private MapWritable currentValue;
        private long size = 0;

        // default constructor used by the NEW api
        ShardRecordReader() {
        }

        // constructor used by the old API
        ShardRecordReader(org.apache.hadoop.mapred.InputSplit split, Configuration job, Reporter reporter) {
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

            // initialize REST client
            client = new BufferedRestClient(settings);

            queryBuilder = QueryBuilder.query(settings.getTargetResource())
                    .shard(esSplit.shardId)
                    .onlyNode(esSplit.nodeId)
                    .time(settings.getScrollKeepAlive())
                    .size(settings.getScrollSize());

            if (log.isDebugEnabled()) {
                log.debug(String.format("Initializing RecordReader from [%s]", esSplit));
            }
        }

        @Override
        public boolean nextKeyValue() throws IOException {
            return next(null, null);
        }

        @Override
        public Text getCurrentKey() throws IOException {
            // new API clients can use the object as is so do a copy
            return new Text(currentKey);
        }

        @Override
        public MapWritable getCurrentValue() {
            // new API clients can use the object as is so do a copy
            return currentValue;
        }

        @Override
        public float getProgress() {
            return size == 0 ? 0 : ((float) getPos()) / size;
        }

        @Override
        public void close() throws IOException {
            if (result != null) {
                result.close();
                result = null;
            }
            client.close();
        }

        @Override
        public boolean next(Text key, MapWritable value) throws IOException {
            if (result == null) {
                result = queryBuilder.build(client);
                size = result.getSize();

                if (log.isTraceEnabled()) {
                    log.trace(String.format("Received scroll [%s],  size [%d] for query [%s]", result, size, queryBuilder));
                }
            }

            boolean hasNext = result.hasNext();

            if (!hasNext) {
                return false;
            }

            Map<String, Object> next = result.next();
            // we save the key as is since under the old API, we don't have to create a new Text() object
            currentKey = next.get("_id").toString();
            currentValue = (MapWritable) WritableUtils.toWritable(next.get("_source"));

            if (key != null) {
                key.set(currentKey);
            }
            if (value != null) {
                value.clear();
                value.putAll(currentValue);
            }

            // keep on counting
            read++;
            return true;
        }


        @Override
        public Text createKey() {
            // old API does object pooling so return just the naked object
            return new Text();
        }

        @Override
        public MapWritable createValue() {
            // old API does object pooling so return just the naked object
            return new MapWritable();
        }

        @Override
        public long getPos() {
            return read;
        }
    }

    //
    // new API - just delegates to the Old API
    //
    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException {
        JobConf conf = (JobConf) context.getConfiguration();
        return Arrays.asList((InputSplit[]) getSplits(conf, conf.getNumMapTasks()));
    }

    @Override
    public ShardRecordReader createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new ShardRecordReader();
    }


    //
    // Old API
    //
    @Override
    public ShardInputSplit[] getSplits(JobConf job, int numSplits) throws IOException {

        Settings settings = SettingsManager.loadFrom(job);
        BufferedRestClient client = new BufferedRestClient(settings);
        Map<Shard, Node> targetShards = client.getTargetShards();
        client.close();

        if (log.isTraceEnabled()) {
            log.trace("Creating splits for shards " + targetShards);
        }

        ShardInputSplit[] splits = new ShardInputSplit[targetShards.size()];

        int index = 0;
        for (Entry<Shard, Node> entry : targetShards.entrySet()) {
            Shard shard = entry.getKey();
            Node node = entry.getValue();
            splits[index++] =
                    new ShardInputSplit(node.getIpAddress(), node.getHttpPort(), node.getId(), node.getName(), shard.getName());
        }

        log.info(String.format("Created [%d] shard-splits", splits.length));
        return splits;
    }

    @Override
    public ShardRecordReader getRecordReader(org.apache.hadoop.mapred.InputSplit split, JobConf job, Reporter reporter) {
        return new ShardRecordReader(split, job, reporter);
    }
}