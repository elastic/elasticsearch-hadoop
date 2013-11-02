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
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.integration.HdpBootstrap;
import org.elasticsearch.hadoop.mr.ESInputFormat;
import org.junit.Test;

public class MRNewApiSearchTest {

    @Test
    public void testBasicSearch() throws Exception {
        Configuration conf = createConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mrnewapi/save/_search?q=*");

        new Job(conf).waitForCompletion(true);
    }

    @Test
    public void testSearchWithId() throws Exception {
        Configuration conf = createConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mrnewapi/savewithid/_search?q=*");

        new Job(conf).waitForCompletion(true);
    }

    @Test
    public void testSearchNonExistingIndex() throws Exception {
        Configuration conf = createConf();
        conf.setBoolean(ConfigurationOptions.ES_INDEX_READ_MISSING_AS_EMPTY, true);
        conf.set(ConfigurationOptions.ES_RESOURCE, "foobar/save/_search?q=*");

        new Job(conf).waitForCompletion(true);
    }

    @Test
    public void testSearchCreated() throws Exception {
        Configuration conf = createConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mrnewapi/createwithid/_search?q=*");

        new Job(conf).waitForCompletion(true);
    }

    @Test
    public void testSearchUpdated() throws Exception {
        Configuration conf = createConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mrnewapi/update/_search?q=*");

        new Job(conf).waitForCompletion(true);
    }

    @Test(expected = IllegalStateException.class)
    public void testSearchUpdatedWithoutUpsertMeaningNonExistingIndex() throws Exception {
        Configuration conf = createConf();
        conf.setBoolean(ConfigurationOptions.ES_INDEX_READ_MISSING_AS_EMPTY, false);
        conf.set(ConfigurationOptions.ES_RESOURCE, "mrnewapi/updatewoupsert/_search?q=*");

        new Job(conf).waitForCompletion(true);
    }

    private Configuration createConf() throws IOException {
        Configuration conf = HdpBootstrap.hadoopConfig();
        conf.setBoolean("mapred.used.genericoptionsparser", true);
        Job job = new Job(conf);
        job.setInputFormatClass(ESInputFormat.class);
        job.setOutputFormatClass(PrintStreamOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapWritable.class);
        job.setNumReduceTasks(0);
        //PrintStreamOutputFormat.stream(conf, Stream.OUT);
        return job.getConfiguration();
    }
}