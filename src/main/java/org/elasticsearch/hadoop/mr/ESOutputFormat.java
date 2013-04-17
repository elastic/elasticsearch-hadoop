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

import java.io.IOException;

import org.apache.commons.lang.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Progressable;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.cfg.SettingsManager;
import org.elasticsearch.hadoop.rest.BufferedRestClient;

/**
 * ElasticSearch {@link OutputFormat} (old and new API) for adding data to an index inside ElasticSearch.
 */
public class ESOutputFormat extends OutputFormat<Object, Object> implements org.apache.hadoop.mapred.OutputFormat<Object, Object>, ConfigurationOptions {

    // don't use mapred.OutputCommitter as it performs mandatory casts to old API resulting in CCE
    public static class ESOutputCommitter extends org.apache.hadoop.mapreduce.OutputCommitter {

        @Override
        public void setupJob(JobContext jobContext) throws IOException {
            //no-op
        }

        @Override
        public void setupTask(TaskAttemptContext taskContext) throws IOException {
            //no-op
        }

        @Override
        public boolean needsTaskCommit(TaskAttemptContext taskContext) throws IOException {
            //no-op
            return false;
        }

        @Override
        public void commitTask(TaskAttemptContext taskContext) throws IOException {
            //no-op
        }

        @Override
        public void abortTask(TaskAttemptContext taskContext) throws IOException {
            //no-op
        }
    }

    public static class ESOldAPIOutputCommitter extends org.apache.hadoop.mapred.OutputCommitter {

        @Override
        public void setupJob(org.apache.hadoop.mapred.JobContext jobContext) throws IOException {
            //no-op
        }

        @Override
        public void setupTask(org.apache.hadoop.mapred.TaskAttemptContext taskContext) throws IOException {
            //no-op
        }

        @Override
        public boolean needsTaskCommit(org.apache.hadoop.mapred.TaskAttemptContext taskContext) throws IOException {
            //no-op
            return false;
        }

        @Override
        public void commitTask(org.apache.hadoop.mapred.TaskAttemptContext taskContext) throws IOException {
            //no-op
        }

        @Override
        public void abortTask(org.apache.hadoop.mapred.TaskAttemptContext taskContext) throws IOException {
            //no-op
        }
    }

    protected static class ESRecordWriter extends RecordWriter<Object, Object> implements org.apache.hadoop.mapred.RecordWriter<Object, Object> {

        private final BufferedRestClient client;

        public ESRecordWriter(Configuration cfg) {
            client = new BufferedRestClient(SettingsManager.loadFrom(cfg));
        }

        @Override
        public void write(Object key, Object value) throws IOException {
            client.addToIndex(value);
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException {
            close((Reporter) null);
        }

        @Override
        public void close(Reporter reporter) throws IOException {
            client.close();
        }
    }

    //
    // new API - just delegates to the Old API
    //
    @Override
    public ESRecordWriter getRecordWriter(TaskAttemptContext context) {
        return getRecordWriter(null, (JobConf) context.getConfiguration(), null, context);
    }

    @Override
    public void checkOutputSpecs(JobContext context) {
        checkOutputSpecs(null, (JobConf) context.getConfiguration());
    }

    @Override
    public org.apache.hadoop.mapreduce.OutputCommitter getOutputCommitter(TaskAttemptContext context) {
        return new ESOutputCommitter();
    }

    //
    // old API
    //
    @Override
    public ESRecordWriter getRecordWriter(FileSystem ignored, JobConf job, String name, Progressable progress) {
        return new ESRecordWriter(job);
    }

    @Override
    public void checkOutputSpecs(FileSystem ignored, JobConf cfg) {
        Settings settings = SettingsManager.loadFrom(cfg);

        Validate.notEmpty(settings.getTargetResource(), String.format("No resource ['%s'] (index/query/location) specified", ES_RESOURCE));
    }
}