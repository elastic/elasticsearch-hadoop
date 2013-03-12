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
import java.util.Collections;
import java.util.List;
import java.util.Map;

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
import org.elasticsearch.hadoop.rest.RestClient;
import org.elasticsearch.hadoop.util.ConfigUtils;
import org.elasticsearch.hadoop.util.WritableUtils;

/**
 * ElasticSearch {@link InputFormat} for streaming data (typically based on a query) from ElasticSearch.
 * Returns the document ID as key and its content as value.
 *
 * <p/>This class implements both the "old" (<tt>org.apache.hadoop.mapred</tt>) and the "new" (<tt>org.apache.hadoop.mapreduce</tt>) API.
 */
public class ESInputFormat extends InputFormat<Text, MapWritable> implements
        org.apache.hadoop.mapred.InputFormat<Text, MapWritable>, ESConfigConstants {

    static class ESInputSplit extends InputSplit implements org.apache.hadoop.mapred.InputSplit {
        private int from = 0;
        private int size = 3;

        public ESInputSplit() {
        }

        public ESInputSplit(int from, int size) {
            this.from = from;
            this.size = size;
        }

        @Override
        public long getLength() {
            return size;
        }

        @Override
        public String[] getLocations() {
            return new String[0];
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(from);
            out.writeInt(size);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            from = in.readInt();
            size = in.readInt();
        }
    }

    // read data in small bulks (through the scan api)
    static class ESRecordReader extends RecordReader<Text, MapWritable> implements
            org.apache.hadoop.mapred.RecordReader<Text, MapWritable> {
        // number of records to get in one call
        private final int BATCH_SIZE = 1;

        private int from;
        private int size;
        private String query;

        private int read = 0;

        private RestClient client;

        private List<Map<String, Object>> batch = Collections.emptyList();
        private int index = 0;
        private boolean done = false;

        // minor optimization - see below
        private String currentKey;
        private MapWritable currentValue;

        // default constructor used by the NEW api
        ESRecordReader() {
        }

        // constructor used by the old API
        ESRecordReader(org.apache.hadoop.mapred.InputSplit split, Configuration job, Reporter reporter) {
            reporter.setStatus(split.toString());
            init((ESInputSplit) split, job);
        }

        // new API init call
        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
            context.setStatus(split.toString());
            init((ESInputSplit) split, context.getConfiguration());
        }

        void init(ESInputSplit esSplit, Configuration cfg) {
            from = esSplit.from;
            size = esSplit.size;

            query = cfg.get(ES_QUERY);
            // initialize REST client
            client = new RestClient(ConfigUtils.detectHostPortAddress(cfg));
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
            return ((float) getPos()) / size;
        }

        @Override
        public void close() throws IOException {
            client.close();
        }

        @Override
        public boolean next(Text key, MapWritable value) throws IOException {
            if (done || read > size) {
                return false;
            }
            // get a new batch
            if (batch.isEmpty() || ++index >= batch.size()) {
                batch = client.query(query, from + read, BATCH_SIZE);
                if (batch.isEmpty()) {
                    done = true;
                    return false;
                }
                // reset index
                index = 0;
            }

            // we save the key as is since under the old API, we don't have to create a new Text() object
            currentKey = batch.get(index).get("_id").toString();
            currentValue = (MapWritable) WritableUtils.toWritable(batch.get(index).get("_source"));

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
    public List<InputSplit> getSplits(JobContext context) {
        JobConf conf = (JobConf) context.getConfiguration();
        return Arrays.asList((InputSplit[]) getSplits(conf, conf.getNumMapTasks()));
    }

    @Override
    public ESRecordReader createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new ESRecordReader();
    }


    //
    // Old API
    //
    @Override
    public ESInputSplit[] getSplits(JobConf job, int numSplits) {
        //TODO: see whether we can rely on Hadoop/Pig for the number of nodes or fallback to our config
        //TODO: ideally we could get the result set and then try to divide this per node
        return new ESInputSplit[] { new ESInputSplit() };
    }

    @Override
    public ESRecordReader getRecordReader(org.apache.hadoop.mapred.InputSplit split, JobConf job, Reporter reporter) {
        return new ESRecordReader(split, job, reporter);
    }
}
