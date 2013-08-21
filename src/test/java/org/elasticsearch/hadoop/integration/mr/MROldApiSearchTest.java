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

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.integration.HdpBootstrap;
import org.elasticsearch.hadoop.mr.ESInputFormat;
import org.junit.Test;

public class MROldApiSearchTest {

    @Test
    public void testBasicSearch() throws Exception {
        JobConf conf = HdpBootstrap.hadoopConfig();

        conf.setInputFormat(ESInputFormat.class);
        conf.setOutputFormat(PrintStreamOutputFormat.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(MapWritable.class);
        conf.setBoolean("mapred.used.genericoptionsparser", true);
        conf.set("es.resource", "mroldapi/save/_search?q=*");

        // un-comment to print results to the console (works only in local mode)
        //PrintStreamOutputFormat.stream(conf, Stream.OUT);

        JobClient.runJob(conf);
    }

    @Test
    public void testSearchNonExistingIndex() throws Exception {
        JobConf conf = HdpBootstrap.hadoopConfig();

        conf.setInputFormat(ESInputFormat.class);
        conf.setOutputFormat(PrintStreamOutputFormat.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(MapWritable.class);
        conf.setBoolean("mapred.used.genericoptionsparser", true);
        conf.setBoolean(ConfigurationOptions.ES_INDEX_READ_MISSING_AS_EMPTY, true);
        conf.set("es.resource", "foobar/save/_search?q=*");

        // un-comment to print results to the console (works only in local mode)
        //PrintStreamOutputFormat.stream(conf, Stream.OUT);

        JobClient.runJob(conf);
    }
}
