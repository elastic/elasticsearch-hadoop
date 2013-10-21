package org.elasticsearch.hadoop.pig;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.elasticsearch.hadoop.mr.ESInputFormat;

@SuppressWarnings("rawtypes")
public class ESPigInputFormat extends ESInputFormat<String, Map> {

    protected static class PigShardRecordReader extends ShardRecordReader<String, Map> {

        public PigShardRecordReader() {
            super();
        }

        public PigShardRecordReader(org.apache.hadoop.mapred.InputSplit split, Configuration job, Reporter reporter) {
            super(split, job, reporter);
        }

        @Override
        public String createKey() {
            return "";
        }

        @Override
        public Map createValue() {
            return new LinkedHashMap();
        }

        @Override
        protected String setCurrentKey(String oldApiKey, String newApiKey, Object object) {
            oldApiKey = object.toString();
            newApiKey = oldApiKey;
            return oldApiKey;
        }

        @SuppressWarnings("unchecked")
        @Override
        protected Map setCurrentValue(Map oldApiValue, Map newApiKey, Object object) {
            Map map = (Map) object;
            if (oldApiValue != null) {
                oldApiValue.clear();
                oldApiValue.putAll(map);
            }
            else {
                oldApiValue = map;
            }
            newApiKey = map;
            return oldApiValue;
        }
    }

    @Override
    public PigShardRecordReader createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new PigShardRecordReader();
    }

    @Override
    public PigShardRecordReader getRecordReader(org.apache.hadoop.mapred.InputSplit split, JobConf job, Reporter reporter) {
        return new PigShardRecordReader(split, job, reporter);
    }
}