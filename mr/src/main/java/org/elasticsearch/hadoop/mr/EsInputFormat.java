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
package org.elasticsearch.hadoop.mr;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Progressable;
import org.elasticsearch.hadoop.cfg.HadoopSettings;
import org.elasticsearch.hadoop.cfg.HadoopSettingsManager;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.mr.compat.CompatHandler;
import org.elasticsearch.hadoop.mr.security.HadoopUserProvider;
import org.elasticsearch.hadoop.rest.InitializationUtils;
import org.elasticsearch.hadoop.rest.PartitionDefinition;
import org.elasticsearch.hadoop.rest.RestRepository;
import org.elasticsearch.hadoop.rest.RestService;
import org.elasticsearch.hadoop.rest.RestService.PartitionReader;
import org.elasticsearch.hadoop.rest.ScrollQuery;
import org.elasticsearch.hadoop.rest.SearchRequestBuilder;
import org.elasticsearch.hadoop.rest.stats.Stats;
import org.elasticsearch.hadoop.serialization.ScrollReader;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * ElasticSearch {@link InputFormat} for streaming data (typically based on a query) from ElasticSearch.
 * Returns the document ID as key and its content as value.
 *
 * <p/>This class implements both the "old" (<tt>org.apache.hadoop.mapred</tt>) and the "new" (<tt>org.apache.hadoop.mapreduce</tt>) API.
 */
public class EsInputFormat<K, V> extends InputFormat<K, V> implements org.apache.hadoop.mapred.InputFormat<K, V>{

    private static Log log = LogFactory.getLog(EsInputFormat.class);

    protected static class EsInputSplit extends InputSplit implements org.apache.hadoop.mapred.InputSplit {
        private PartitionDefinition partition;

        public EsInputSplit() {}

        public EsInputSplit(PartitionDefinition partition) {
            this.partition = partition;
        }

        @Override
        public long getLength() {
            // TODO: can this be computed easily?
            return 1l;
        }

        @Override
        public String[] getLocations() {
            return partition.getHostNames();
        }

        @Override
        public void write(DataOutput out) throws IOException {
            partition.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            partition = new PartitionDefinition(in);
        }

        public PartitionDefinition getPartition() {
            return partition;
        }

        @Override
        public String toString() {
            return "EsInputSplit{" +
                    (partition == null ? "NULL" : partition.toString()) +
                    "}";
        }
    }

    protected static abstract class EsInputRecordReader<K,V> extends RecordReader<K, V> implements org.apache.hadoop.mapred.RecordReader<K, V> {

        private int read = 0;
        private EsInputSplit esSplit;
        private ScrollReader scrollReader;

        private RestRepository client;
        private SearchRequestBuilder queryBuilder;
        private ScrollQuery scrollQuery;

        // reuse objects
        private K currentKey;
        private V currentValue;

        private long size = 0;

        private HeartBeat beat;
        private Progressable progressable;

        // default constructor used by the NEW api
        public EsInputRecordReader() {
        }

        // constructor used by the old API
        public EsInputRecordReader(org.apache.hadoop.mapred.InputSplit split, Configuration job, Reporter reporter) {
            reporter.setStatus(split.toString());
            init((EsInputSplit) split, job, reporter);
        }

        // new API init call
        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
            org.elasticsearch.hadoop.mr.compat.TaskAttemptContext compatContext = CompatHandler.taskAttemptContext(context);
            compatContext.setStatus(split.toString());
            init((EsInputSplit) split, compatContext.getConfiguration(), compatContext);
        }

        void init(EsInputSplit esSplit, Configuration cfg, Progressable progressable) {
            // get a copy to override the host/port
            Settings settings = HadoopSettingsManager.loadFrom(cfg).copy().load(esSplit.getPartition().getSerializedSettings());

            if (log.isTraceEnabled()) {
                log.trace(String.format("Init shard reader from cfg %s", HadoopCfgUtils.asProperties(cfg)));
                log.trace(String.format("Init shard reader w/ settings %s", settings));
            }

            this.esSplit = esSplit;

            // initialize mapping/ scroll reader
            InitializationUtils.setValueReaderIfNotSet(settings, WritableValueReader.class, log);
            InitializationUtils.setUserProviderIfNotSet(settings, HadoopUserProvider.class, log);

            PartitionDefinition part = esSplit.getPartition();
            PartitionReader partitionReader = RestService.createReader(settings, part, log);

            this.scrollReader = partitionReader.scrollReader;
            this.client = partitionReader.client;
            this.queryBuilder = partitionReader.queryBuilder;

            this.progressable = progressable;

            // in Hadoop-like envs (Spark) the progressable might be null and thus the heart-beat is not needed
            if (progressable != null) {
                beat = new HeartBeat(progressable, cfg, settings.getHeartBeatLead(), log);
            }

            if (log.isDebugEnabled()) {
                log.debug(String.format("Initializing RecordReader for [%s]", esSplit));
            }
        }

        @Override
        public boolean nextKeyValue() throws IOException {
            // new API call routed to old API
            // under the new API always create new objects since consumers can (and sometimes will) modify them

            currentKey = createKey();
            currentValue = createValue();

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
            try {
                if (log.isDebugEnabled()) {
                    log.debug(String.format("Closing RecordReader for [%s]", esSplit));
                }

                if (beat != null) {
                    beat.stop();
                }

                if (scrollQuery != null) {
                    scrollQuery.close();
                }

                if (client != null) {
                    client.close();
                }

            } finally {
                Stats stats = new Stats();
                if (client != null) {
                    stats.aggregate(client.stats());
                    client = null;
                }
                if (scrollQuery != null) {
                    stats.aggregate(scrollQuery.stats());
                    scrollQuery = null;
                }
                ReportingUtils.report(progressable, stats);
            }
        }

        @Override
        public boolean next(K key, V value) throws IOException {
            if (scrollQuery == null) {
                if (beat != null) {
                    beat.start();
                }

                scrollQuery = queryBuilder.build(client, scrollReader);
                size = scrollQuery.getSize();

                if (log.isTraceEnabled()) {
                    log.trace(String.format("Received scroll [%s],  size [%d] for query [%s]", scrollQuery, size, queryBuilder));
                }
            }

            boolean hasNext = scrollQuery.hasNext();

            if (!hasNext) {
                return false;
            }

            Object[] next = scrollQuery.next();

            // NB: the left assignment is not needed since method override
            // the writable content however for consistency, they are below
            currentKey = setCurrentKey(key, next[0]);
            currentValue = setCurrentValue(value, next[1]);

            // keep on counting
            read++;
            return true;
        }

        @Override
        public abstract K createKey();

        @Override
        public abstract V createValue();

        /**
         * Sets the current key.
         *
         * @param hadoopKey hadoop key
         * @param object the actual value to read
         * @return returns the key to be used; needed in scenario where the key is immutable (like Pig)
         */
        protected abstract K setCurrentKey(K hadoopKey, Object object);

        /**
         * Sets the current value.
         *
         * @param hadoopValue hadoop value
         * @param object the actual value to read
         * @return returns the value to be used; needed in scenario where the passed value is immutable (like Pig)
         */
        protected abstract V setCurrentValue(V hadoopValue, Object object);

        @Override
        public long getPos() {
            return read;
        }
    }

    protected static abstract class AbstractWritableEsInputRecordReader<V> extends EsInputRecordReader<Text, V> {

        public AbstractWritableEsInputRecordReader() {
            super();
        }

        public AbstractWritableEsInputRecordReader(org.apache.hadoop.mapred.InputSplit split, Configuration job, Reporter reporter) {
            super(split, job, reporter);
        }

        @Override
        public Text createKey() {
            return new Text();
        }

        @Override
        protected Text setCurrentKey(Text hadoopKey, Object object) {
            if (hadoopKey != null) {
                hadoopKey.set(object.toString());
            }
            return hadoopKey;
        }
    }

    protected static class WritableEsInputRecordReader extends AbstractWritableEsInputRecordReader<Map<Writable, Writable>> {

        private boolean useLinkedMapWritable = true;

        public WritableEsInputRecordReader() {
            super();
        }

        public WritableEsInputRecordReader(org.apache.hadoop.mapred.InputSplit split, Configuration job, Reporter reporter) {
            super(split, job, reporter);
        }


        @Override
        void init(EsInputSplit esSplit, Configuration cfg, Progressable progressable) {
            useLinkedMapWritable = (!MapWritable.class.getName().equals(HadoopCfgUtils.getMapValueClass(cfg)));
            super.init(esSplit, cfg, progressable);
        }

        @Override
        public Map<Writable, Writable> createValue() {
            return (useLinkedMapWritable ? new LinkedMapWritable() : new MapWritable());
        }

        @SuppressWarnings("unchecked")
        @Override
        protected Map<Writable, Writable> setCurrentValue(Map<Writable, Writable> hadoopValue, Object object) {
            if (hadoopValue != null) {
                hadoopValue.clear();
                Map<Writable, Writable> val = (Map<Writable, Writable>) object;
                hadoopValue.putAll(val);
            }
            return hadoopValue;
        }
    }

    protected static class JsonWritableEsInputRecordReader extends AbstractWritableEsInputRecordReader<Text> {

        public JsonWritableEsInputRecordReader() {
            super();
        }

        public JsonWritableEsInputRecordReader(org.apache.hadoop.mapred.InputSplit split, Configuration job,
                                               Reporter reporter) {
            super(split, job, reporter);
        }

        @Override
        public Text createValue() {
            return new Text();
        }

        @Override
        protected Text setCurrentValue(Text hadoopValue, Object object) {
            if (hadoopValue != null) {
                hadoopValue.set(object.toString());
            }
            return hadoopValue;
        }
    }

    //
    // new API - just delegates to the Old API
    //
    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException {
        JobConf conf = HadoopCfgUtils.asJobConf(CompatHandler.jobContext(context).getConfiguration());
        // NOTE: this method expects a ShardInputSplit to be returned (which implements both the old and the new API).
        return Arrays.asList((InputSplit[]) getSplits(conf, conf.getNumMapTasks()));
    }

    @SuppressWarnings("unchecked")
    @Override
    public EsInputRecordReader<K, V> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return (EsInputRecordReader<K, V>) (isOutputAsJson(CompatHandler.taskAttemptContext(context).getConfiguration()) ? new JsonWritableEsInputRecordReader() : new WritableEsInputRecordReader());
    }


    //
    // Old API - if this method is replaced, make sure to return a new/old-API compatible InputSplit
    //

    // Note: data written to the JobConf will be silently discarded
    @Override
    @Deprecated // Hadoop 1 support is deprecated
    public org.apache.hadoop.mapred.InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {

        Settings settings = HadoopSettingsManager.loadFrom(job);
        Collection<PartitionDefinition> partitions = RestService.findPartitions(settings, log);
        EsInputSplit[] splits = new EsInputSplit[partitions.size()];

        int index = 0;
        for (PartitionDefinition part : partitions) {
            splits[index++] = new EsInputSplit(part);
        }
        log.info(String.format("Created [%d] splits", splits.length));
        return splits;
    }

    @SuppressWarnings("unchecked")
    @Override
    @Deprecated // Hadoop 1 support is deprecated
    public EsInputRecordReader<K, V> getRecordReader(org.apache.hadoop.mapred.InputSplit split, JobConf job, Reporter reporter) {
        return (EsInputRecordReader<K, V>) (isOutputAsJson(job) ? new JsonWritableEsInputRecordReader(split, job, reporter) : new WritableEsInputRecordReader(split, job, reporter));
    }

    protected boolean isOutputAsJson(Configuration cfg) {
        return new HadoopSettings(cfg).getOutputAsJson();
    }
}