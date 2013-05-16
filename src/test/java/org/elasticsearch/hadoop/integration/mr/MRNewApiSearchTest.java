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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.elasticsearch.hadoop.integration.TestSettings;
import org.elasticsearch.hadoop.mr.ESInputFormat;
import org.elasticsearch.hadoop.util.TestUtils;
import org.junit.Test;

public class MRNewApiSearchTest {

    @Test
    public void testBasicSearch() throws Exception {
        Configuration conf = new Configuration();
        TestUtils.addProperties(conf, TestSettings.TESTING_PROPS);
        conf.setBoolean("mapred.used.genericoptionsparser", true);
        conf.set("mapred.job.tracker", "local");
        conf.set("es.resource", "mrnewapi/save/_search?q=*");

        Job job = new Job(conf);
        job.setInputFormatClass(ESInputFormat.class);
        job.setOutputFormatClass(PrintStreamOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapWritable.class);

        //PrintStreamOutputFormat.stream(conf, Stream.OUT);

        job.waitForCompletion(true);
    }
}
