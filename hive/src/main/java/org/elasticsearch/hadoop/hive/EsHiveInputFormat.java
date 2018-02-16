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
package org.elasticsearch.hadoop.hive;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.elasticsearch.hadoop.cfg.HadoopSettingsManager;
import org.elasticsearch.hadoop.cfg.InternalConfigurationOptions;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.mr.EsInputFormat;
import org.elasticsearch.hadoop.rest.InitializationUtils;
import org.elasticsearch.hadoop.util.StringUtils;

/**
 * Hive specific InputFormat. Since Hive code base makes a lot of assumptions about the tables being actual files in HDFS (using instanceof checks without proper else) this class tries to 'fix' this by
 * adding a dummy {@link FileInputFormat} to ESInputFormat.
 */

// A quick example would be {@link org.apache.hadoop.hive.ql.io.HiveInputFormat.HiveInputSplit#getPath()} which, in case the actual InputSplit is not a
// {@link org.apache.hadoop.mapred.FileSplit}, returns an invalid Path.

public class EsHiveInputFormat extends EsInputFormat<Text, Writable> {

    static class EsHiveSplit extends FileSplit {
        InputSplit delegate;
        private Path path;

        EsHiveSplit() {
            this(new EsInputSplit(), null);
        }

        EsHiveSplit(InputSplit delegate, Path path) {
            super(path, 0, 0, (String[]) null);
            this.delegate = delegate;
            this.path = path;
        }

        public long getLength() {
            // TODO: can this be delegated?
            return 1L;
        }

        public String[] getLocations() throws IOException {
            return delegate.getLocations();
        }

        public void write(DataOutput out) throws IOException {
            Text.writeString(out, path.toString());
            delegate.write(out);
        }

        public void readFields(DataInput in) throws IOException {
            path = new Path(Text.readString(in));
            delegate.readFields(in);
        }

        @Override
        public String toString() {
            return delegate.toString();
        }

        @Override
        public Path getPath() {
            return path;
        }
    }

    @Override
    public FileSplit[] getSplits(JobConf job, int numSplits) throws IOException {
        // first, merge input table properties (since there's no access to them ...)
        Settings settings = HadoopSettingsManager.loadFrom(job);
        //settings.merge(IOUtils.propsFromString(settings.getProperty(HiveConstants.INPUT_TBL_PROPERTIES)));

        Log log = LogFactory.getLog(getClass());
        // move on to initialization
        InitializationUtils.setValueReaderIfNotSet(settings, HiveValueReader.class, log);
        if (settings.getOutputAsJson() == false) {
            // Only set the fields if we aren't asking for raw JSON
            settings.setProperty(InternalConfigurationOptions.INTERNAL_ES_TARGET_FIELDS, StringUtils.concatenate(HiveUtils.columnToAlias(settings), ","));
        }

        HiveUtils.init(settings, log);

        // decorate original splits as FileSplit
        InputSplit[] shardSplits = super.getSplits(job, numSplits);
        FileSplit[] wrappers = new FileSplit[shardSplits.length];
        Path path = new Path(job.get(HiveConstants.TABLE_LOCATION));
        for (int i = 0; i < wrappers.length; i++) {
            wrappers[i] = new EsHiveSplit(shardSplits[i], path);
        }
        return wrappers;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public AbstractWritableEsInputRecordReader getRecordReader(InputSplit split, JobConf job, Reporter reporter) {
        InputSplit delegate = ((EsHiveSplit) split).delegate;
        return isOutputAsJson(job) ? new JsonWritableEsInputRecordReader(delegate, job, reporter) : new WritableEsInputRecordReader(delegate, job, reporter);
    }
}
