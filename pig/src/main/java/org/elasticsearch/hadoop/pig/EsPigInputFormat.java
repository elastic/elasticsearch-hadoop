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
package org.elasticsearch.hadoop.pig;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.elasticsearch.hadoop.mr.EsInputFormat;
import org.elasticsearch.hadoop.mr.compat.CompatHandler;
import org.elasticsearch.hadoop.util.StringUtils;


@SuppressWarnings("rawtypes")
public class EsPigInputFormat extends EsInputFormat<String, Object> {

    protected static abstract class AbstractPigShardRecordReader<V> extends ShardRecordReader<String, V> {
        public AbstractPigShardRecordReader() {
            super();
        }

        public AbstractPigShardRecordReader(org.apache.hadoop.mapred.InputSplit split, Configuration job, Reporter reporter) {
            super(split, job, reporter);
        }

        @Override
        public String createKey() {
            return StringUtils.EMPTY;
        }

        @Override
        protected String setCurrentKey(String hadoopKey, Object object) {
            // cannot override a String content (recipe for disaster)
            // in case of Pig, it's okay to return a new object as it's using the new API
            return object.toString();
        }
    }

    protected static class PigShardRecordReader extends AbstractPigShardRecordReader<Map> {

        public PigShardRecordReader() {
            super();
        }

        public PigShardRecordReader(org.apache.hadoop.mapred.InputSplit split, Configuration job, Reporter reporter) {
            super(split, job, reporter);
        }

        @Override
        public Map createValue() {
            return new LinkedHashMap();
        }

        @SuppressWarnings("unchecked")
        @Override
        protected Map setCurrentValue(Map hadoopValue, Object object) {
            Map map = (Map) object;
            if (hadoopValue != null) {
                hadoopValue.clear();
                hadoopValue.putAll(map);
            }
            return hadoopValue;
        }
    }

    protected static class PigJsonShardRecordReader extends AbstractPigShardRecordReader<String> {

        public PigJsonShardRecordReader() {
            super();
        }

        public PigJsonShardRecordReader(org.apache.hadoop.mapred.InputSplit split, Configuration job, Reporter reporter) {
            super(split, job, reporter);
        }

        @Override
        public String createValue() {
            return StringUtils.EMPTY;
        }

        @Override
        protected String setCurrentValue(String hadoopValue, Object object) {
            return object.toString();
        }
    }


    @SuppressWarnings("unchecked")
    @Override
    public AbstractPigShardRecordReader createRecordReader(InputSplit split, TaskAttemptContext context) {
        return isOutputAsJson(CompatHandler.taskAttemptContext(context).getConfiguration()) ? new PigJsonShardRecordReader() : new PigShardRecordReader();
    }

    @SuppressWarnings("unchecked")
    @Override
    public AbstractPigShardRecordReader getRecordReader(org.apache.hadoop.mapred.InputSplit split, JobConf job, Reporter reporter) {
        return isOutputAsJson(job) ? new PigJsonShardRecordReader(split, job, reporter) : new PigShardRecordReader(split, job, reporter);
    }
}