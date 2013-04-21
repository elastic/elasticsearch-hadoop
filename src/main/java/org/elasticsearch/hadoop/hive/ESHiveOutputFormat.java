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
package org.elasticsearch.hadoop.hive;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.elasticsearch.hadoop.mr.ESOutputFormat;

/**
 * Hive specific OutputFormat.
 */
public class ESHiveOutputFormat extends ESOutputFormat implements HiveOutputFormat<Object, Object> {

    static class ESHiveRecordWriter extends ESOutputFormat.ESRecordWriter implements RecordWriter {

        public ESHiveRecordWriter(Configuration cfg) {
            super(cfg);
        }

        @Override
        public void write(Writable w) throws IOException {
            super.write(null, w);
        }

        @Override
        public void close(boolean abort) throws IOException {
            // TODO: check whether a proper Reporter can be passed in
            super.close((Reporter) null);
        }
    }

    @Override
    public RecordWriter getHiveRecordWriter(JobConf jc, Path finalOutPath, Class<? extends Writable> valueClass, boolean isCompressed, Properties tableProperties, Progressable progress) {
        return new ESHiveRecordWriter(jc);
    }
}
