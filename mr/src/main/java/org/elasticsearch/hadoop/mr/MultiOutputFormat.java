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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Progressable;
import org.elasticsearch.hadoop.mr.compat.CompatHandler;

public class MultiOutputFormat extends OutputFormat implements org.apache.hadoop.mapred.OutputFormat {

    private static class MultiNewRecordWriter extends RecordWriter {

        private final List<RecordWriter> writers;

        public MultiNewRecordWriter(List<RecordWriter> writers) {
            this.writers = writers;
        }

        @Override
        public void write(Object key, Object value) throws IOException, InterruptedException {
            for (RecordWriter writer : writers) {
                writer.write(key, value);
            }
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            for (RecordWriter writer : writers) {
                writer.close(context);
            }
        }
    }

    private static class MultiOldRecordWriter implements org.apache.hadoop.mapred.RecordWriter {

        private final List<org.apache.hadoop.mapred.RecordWriter> writers;

        public MultiOldRecordWriter(List<org.apache.hadoop.mapred.RecordWriter> writers) {
            this.writers = writers;
        }

        @Override
        public void close(Reporter reporter) throws IOException {
            for (org.apache.hadoop.mapred.RecordWriter writer : writers) {
                writer.close(reporter);
            }
        }


        @Override
        public void write(Object key, Object value) throws IOException {
            for (org.apache.hadoop.mapred.RecordWriter writer : writers) {
                writer.write(key, value);
            }
        }
    }

    private static class MultiNewOutputCommitter extends org.apache.hadoop.mapreduce.OutputCommitter {

        private final List<org.apache.hadoop.mapreduce.OutputCommitter> committers;

        MultiNewOutputCommitter(List<org.apache.hadoop.mapreduce.OutputCommitter> committers) {
            this.committers = committers;
        }

        @Override
        public void setupJob(JobContext jobContext) throws IOException {
            for (OutputCommitter committer : committers) {
                committer.setupJob(jobContext);
            }
        }

        @Override
        public void setupTask(TaskAttemptContext taskContext) throws IOException {
            for (OutputCommitter committer : committers) {
                committer.setupTask(taskContext);
            }
        }

        @Override
        public boolean needsTaskCommit(TaskAttemptContext taskContext) throws IOException {
            boolean result = false;

            for (OutputCommitter committer : committers) {
                result |= committer.needsTaskCommit(taskContext);
            }

            return result;
        }

        @Override
        public void commitTask(TaskAttemptContext taskContext) throws IOException {
            for (OutputCommitter committer : committers) {
                committer.commitTask(taskContext);
            }
        }

        @Override
        public void abortTask(TaskAttemptContext taskContext) throws IOException {
            for (OutputCommitter committer : committers) {
                committer.abortTask(taskContext);
            }
        }
    }

    private static class MultiOldOutputCommitter extends org.apache.hadoop.mapred.OutputCommitter {

        private final List<org.apache.hadoop.mapred.OutputCommitter> committers;

        MultiOldOutputCommitter(List<org.apache.hadoop.mapred.OutputCommitter> committers) {
            this.committers = committers;
        }


        @Override
        public void setupJob(org.apache.hadoop.mapred.JobContext jobContext) throws IOException {
            for (OutputCommitter committer : committers) {
                committer.setupJob(jobContext);
            }
        }

        @Override
        public void setupTask(org.apache.hadoop.mapred.TaskAttemptContext taskContext) throws IOException {
            for (OutputCommitter committer : committers) {
                committer.setupTask(taskContext);
            }
        }

        @Override
        public boolean needsTaskCommit(org.apache.hadoop.mapred.TaskAttemptContext taskContext) throws IOException {
            boolean result = false;

            for (OutputCommitter committer : committers) {
                result |= committer.needsTaskCommit(taskContext);
            }

            return result;
        }

        @Override
        public void commitTask(org.apache.hadoop.mapred.TaskAttemptContext taskContext) throws IOException {
            for (OutputCommitter committer : committers) {
                committer.commitTask(taskContext);
            }
        }

        @Override
        public void abortTask(org.apache.hadoop.mapred.TaskAttemptContext taskContext) throws IOException {
            for (OutputCommitter committer : committers) {
                committer.abortTask(taskContext);
            }
        }

        @Override
        @Deprecated
        public void cleanupJob(org.apache.hadoop.mapred.JobContext context) throws IOException {
            // no-op
            // added for compatibility with hadoop 0.20.x (used by old tools, such as Cascalog)
            for (OutputCommitter committer : committers) {
                committer.cleanupJob(context);
            }
        }
    }

    public static final String CFG_FIELD = "es.hadoop.multi.of";
    private transient List<OutputFormat> newApiFormat = null;
    private transient List<org.apache.hadoop.mapred.OutputFormat> oldApiFormat = null;


    //
    // Old API
    //
    @Override
    public org.apache.hadoop.mapred.RecordWriter getRecordWriter(FileSystem ignored, JobConf job, String name, Progressable progress)
            throws IOException {

        List<org.apache.hadoop.mapred.OutputFormat> formats = getOldApiFormats(job);
        List<org.apache.hadoop.mapred.RecordWriter> writers = new ArrayList<org.apache.hadoop.mapred.RecordWriter>();
        for (org.apache.hadoop.mapred.OutputFormat format : formats) {
            writers.add(format.getRecordWriter(ignored, job, name, progress));
        }

        return new MultiOldRecordWriter(writers);
    }

    @Override
    public void checkOutputSpecs(FileSystem ignored, JobConf job) throws IOException {
        List<org.apache.hadoop.mapred.OutputFormat> formats = getOldApiFormats(job);
        for (org.apache.hadoop.mapred.OutputFormat format : formats) {
            format.checkOutputSpecs(ignored, job);
        }
    }

    //
    // new API
    //
    @Override
    public RecordWriter getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        List<OutputFormat> formats = getNewApiFormats(CompatHandler.taskAttemptContext(context).getConfiguration());
        List<RecordWriter> writers = new ArrayList<RecordWriter>();
        for (OutputFormat format : formats) {
            writers.add(format.getRecordWriter(context));
        }

        return new MultiNewRecordWriter(writers);
    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
        List<OutputFormat> formats = getNewApiFormats(CompatHandler.jobContext(context).getConfiguration());
        for (OutputFormat format : formats) {
            format.checkOutputSpecs(context);
        }
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        List<OutputFormat> formats = getNewApiFormats(CompatHandler.taskAttemptContext(context).getConfiguration());
        List<OutputCommitter> committers = new ArrayList<OutputCommitter>();
        for (OutputFormat format : formats) {
            committers.add(format.getOutputCommitter(context));
        }

        return new MultiNewOutputCommitter(committers);
    }

    public static void addOutputFormat(Configuration cfg, Class<? extends OutputFormat>... formats) {
        Collection<String> of = cfg.getStringCollection(CFG_FIELD);
        for (Class<? extends OutputFormat> format : formats) {
            of.add(format.getName());
        }
        cfg.setStrings(CFG_FIELD, StringUtils.join(of, ","));
    }

    private List<OutputFormat> getNewApiFormats(Configuration cfg) {
        if (newApiFormat == null) {
            newApiFormat = cfg.getInstances(CFG_FIELD, OutputFormat.class);
        }
        return newApiFormat;
    }

    private List<org.apache.hadoop.mapred.OutputFormat> getOldApiFormats(Configuration cfg) {
        if (oldApiFormat == null) {
            oldApiFormat = cfg.getInstances(CFG_FIELD, org.apache.hadoop.mapred.OutputFormat.class);
        }
        return oldApiFormat;
    }
}