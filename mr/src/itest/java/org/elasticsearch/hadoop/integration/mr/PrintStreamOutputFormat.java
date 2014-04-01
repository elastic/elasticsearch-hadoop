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
package org.elasticsearch.hadoop.integration.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Progressable;
import org.elasticsearch.hadoop.Stream;
import org.elasticsearch.hadoop.mr.EsOutputFormat;
import org.elasticsearch.hadoop.util.WritableUtils;


public class PrintStreamOutputFormat extends org.apache.hadoop.mapreduce.OutputFormat implements OutputFormat {

    private Stream stream;

    private class PrintStreamRecordWriter extends org.apache.hadoop.mapreduce.RecordWriter implements RecordWriter {

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            close((Reporter) null);
        }

        @Override
        public void write(Object key, Object value) throws IOException {
            stream.stream().printf("%s\n", WritableUtils.fromWritable((Writable) value));
        }

        @Override
        public void close(Reporter reporter) throws IOException {
            stream.stream().flush();
        }
    }

    public PrintStreamOutputFormat() {
        this(Stream.NULL);
    }

    public PrintStreamOutputFormat(Stream stream) {
        this.stream = stream;
    }

    // New API
    @Override
    public org.apache.hadoop.mapreduce.RecordWriter getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new PrintStreamRecordWriter();
    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
        // no-op
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new EsOutputFormat.EsOutputCommitter();
    }

    // Old API
    @Override
    public RecordWriter getRecordWriter(FileSystem ignored, JobConf job, String name, Progressable progress)
            throws IOException {
        stream = Stream.valueOf(job.get(Stream.class.getName(), Stream.NULL.name()));
        return new PrintStreamRecordWriter();
    }

    @Override
    public void checkOutputSpecs(FileSystem ignored, JobConf job) throws IOException {
        //no-op
    }

    public static void stream(Configuration cfg, Stream stream) {
        cfg.set(Stream.class.getName(), stream.name());
    }
}