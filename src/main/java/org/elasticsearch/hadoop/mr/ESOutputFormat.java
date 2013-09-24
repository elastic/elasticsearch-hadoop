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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.elasticsearch.hadoop.rest.InitializationUtils;
import org.elasticsearch.hadoop.serialization.SerializationUtils;
import org.elasticsearch.hadoop.util.Assert;

/**
 * ElasticSearch {@link OutputFormat} (old and new API) for adding data to an index inside ElasticSearch.
 */
@SuppressWarnings("rawtypes")
public class ESOutputFormat extends OutputFormat implements org.apache.hadoop.mapred.OutputFormat, ConfigurationOptions {

    private static Log log = LogFactory.getLog(ESOutputFormat.class);

    // don't use mapred.OutputCommitter as it performs mandatory casts to old API resulting in CCE
    public static class ESOutputCommitter extends org.apache.hadoop.mapreduce.OutputCommitter {

        @Override
        public void setupJob(JobContext jobContext) throws IOException {
        }

        // compatibility check with Hadoop 0.20.2
        @Deprecated
        public void cleanupJob(JobContext jobContext) throws IOException {
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

        @Override
        @Deprecated
        public void cleanupJob(org.apache.hadoop.mapred.JobContext context) throws IOException {
            // no-op
            // added for compatibility with hadoop 0.20.x (used by old tools, such as Cascalog)
        }
    }

    protected static class ESRecordWriter extends RecordWriter implements org.apache.hadoop.mapred.RecordWriter {

        protected final BufferedRestClient client;
        private final String uri, resource;

        public ESRecordWriter(Configuration cfg) {
            Settings settings = SettingsManager.loadFrom(cfg);

            SerializationUtils.setValueWriterIfNotSet(settings, WritableValueWriter.class, log);

            client = new BufferedRestClient(settings);
            uri = settings.getTargetUri();
            resource = settings.getTargetResource();
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
            if (log.isTraceEnabled()) {
                log.trace(String.format("Closing RecordWriter [%s][%s]", uri, resource));
            }
            client.close();
        }
    }

    //
    // new API - just delegates to the Old API
    //
    @Override
    public org.apache.hadoop.mapreduce.RecordWriter getRecordWriter(TaskAttemptContext context) {
        return (org.apache.hadoop.mapreduce.RecordWriter) getRecordWriter(null, (JobConf) context.getConfiguration(), null, context);
    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException {
        // careful as it seems the info here saved by in the config is discarded
        init(context.getConfiguration());
    }

    @Override
    public org.apache.hadoop.mapreduce.OutputCommitter getOutputCommitter(TaskAttemptContext context) {
        return new ESOutputCommitter();
    }

    //
    // old API
    //
    @Override
    public org.apache.hadoop.mapred.RecordWriter getRecordWriter(FileSystem ignored, JobConf job, String name, Progressable progress) {
        return new ESRecordWriter(job);
    }

    @Override
    public void checkOutputSpecs(FileSystem ignored, JobConf cfg) throws IOException {
        init(cfg);
    }

    private void init(Configuration cfg) throws IOException {
        Settings settings = SettingsManager.loadFrom(cfg);
        Assert.hasText(settings.getTargetResource(), String.format("No resource ['%s'] (index/query/location) specified", ES_RESOURCE));

        // lazy-init
        BufferedRestClient client = null;

        InitializationUtils.checkIndexExistence(settings, client);

        log.info(String.format("Preparing to write/index to [%s][%s]", settings.getTargetUri(), settings.getTargetResource()));
    }
}