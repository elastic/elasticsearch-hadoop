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
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class MultiOutputFormat extends OutputFormat {

    private static class MultiRecordWriter extends RecordWriter {

        private final List<RecordWriter> writers;

        public MultiRecordWriter(List<RecordWriter> writers) {
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

    private static class MultiOutputCommitter extends OutputCommitter {

        private final List<OutputCommitter> committers;

        MultiOutputCommitter(List<OutputCommitter> committers) {
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


    public static final String CFG_FIELD = "es.hadoop.multi.of";
    private transient List<OutputFormat> formats = null;

    @Override
    public RecordWriter getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        List<OutputFormat> formats = getFormats(context.getConfiguration());
        List<RecordWriter> writers = new ArrayList<RecordWriter>();
        for (OutputFormat format : formats) {
            writers.add(format.getRecordWriter(context));
        }

        return new MultiRecordWriter(writers);
    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
        List<OutputFormat> formats = getFormats(context.getConfiguration());
        for (OutputFormat format : formats) {
            format.checkOutputSpecs(context);
        }
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        List<OutputFormat> formats = getFormats(context.getConfiguration());
        List<OutputCommitter> committers = new ArrayList<OutputCommitter>();
        for (OutputFormat format : formats) {
            committers.add(format.getOutputCommitter(context));
        }

        return new MultiOutputCommitter(committers);
    }

    public static void addOutputFormat(Configuration cfg, Class<? extends OutputFormat>...formats) {
        Collection<String> of = cfg.getStringCollection(CFG_FIELD);
        for (Class<? extends OutputFormat> format : formats) {
            of.add(format.getName());
        }
        cfg.setStrings(CFG_FIELD, StringUtils.join(of, ","));
    }

    private List<OutputFormat> getFormats(Configuration cfg) {
        if (formats == null) {
            formats = cfg.getInstances(CFG_FIELD, OutputFormat.class);
        }
        return formats;
    }
}