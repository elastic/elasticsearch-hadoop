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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.integration.HdpBootstrap;
import org.elasticsearch.hadoop.mr.ESInputFormat;
import org.elasticsearch.hadoop.mr.LinkedMapWritable;
import org.junit.Test;

public class MROldApiSearchTest {

    @Test
    public void testBasicSearch() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi/save/_search?q=*");

        JobClient.runJob(conf);
    }

    @Test
    public void testSearchWithId() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi/savewithid/_search?q=*");

        JobClient.runJob(conf);
    }

    @Test
    public void testSearchNonExistingIndex() throws Exception {
        JobConf conf = createJobConf();
        conf.setBoolean(ConfigurationOptions.ES_INDEX_READ_MISSING_AS_EMPTY, true);
        conf.set(ConfigurationOptions.ES_RESOURCE, "foobar/save/_search?q=*");

        JobClient.runJob(conf);
    }

    @Test
    public void testSearchCreated() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi/createwithid/_search?q=*");

        JobClient.runJob(conf);
    }

    @Test
    public void testSearchUpdated() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi/update/_search?q=*");

        JobClient.runJob(conf);
    }

    @Test(expected = IllegalStateException.class)
    public void testSearchUpdatedWithoutUpsertMeaningNonExistingIndex() throws Exception {
        JobConf conf = createJobConf();
        conf.setBoolean(ConfigurationOptions.ES_INDEX_READ_MISSING_AS_EMPTY, false);
        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi/updatewoupsert/_search?q=*");

        JobClient.runJob(conf);
    }

    private JobConf createJobConf() {
        JobConf conf = HdpBootstrap.hadoopConfig();

        conf.setInputFormat(ESInputFormat.class);
        conf.setOutputFormat(PrintStreamOutputFormat.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(LinkedMapWritable.class);
        conf.setBoolean("mapred.used.genericoptionsparser", true);
        conf.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(conf, new Path("src/test/resources/artists.dat"));
        return conf;
    }
}