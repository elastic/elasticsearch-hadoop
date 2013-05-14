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
package org.elasticsearch.hadoop.integration.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.elasticsearch.hadoop.integration.Stream;
import org.elasticsearch.hadoop.util.WritableUtils;


public class PrintStreamOutputFormat implements OutputFormat {

    private Stream stream;

    public PrintStreamOutputFormat() {
        this(Stream.NULL);
    }

    public PrintStreamOutputFormat(Stream stream) {
        this.stream = stream;
    }

    @Override
    public RecordWriter getRecordWriter(FileSystem ignored, JobConf job, String name, Progressable progress)
            throws IOException {
        stream = Stream.valueOf(job.get(Stream.class.getName(), Stream.NULL.name()));

        return new RecordWriter() {

            @Override
            public void write(Object key, Object value) throws IOException {
                stream.stream().printf("%s\n", WritableUtils.fromWritable((Writable) value));
            }

            @Override
            public void close(Reporter reporter) throws IOException {
                stream.stream().flush();
            }
        };
    }

    @Override
    public void checkOutputSpecs(FileSystem ignored, JobConf job) throws IOException {
        //no-op
    }

    public static void stream(Configuration cfg, Stream stream) {
        cfg.set(Stream.class.getName(), stream.name());
    }
}