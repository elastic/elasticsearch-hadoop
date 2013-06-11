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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.elasticsearch.hadoop.integration.HdpBootstrap;
import org.elasticsearch.hadoop.mr.ESOutputFormat;
import org.elasticsearch.hadoop.util.WritableUtils;
import org.junit.Test;

public class MRNewApiSaveTest {

    public static class JsonMapper extends Mapper {

        @Override
        protected void map(Object key, Object value, Context context) throws IOException, InterruptedException {
            StringTokenizer st = new StringTokenizer(value.toString(), "\t");
            Map<String, String> entry = new LinkedHashMap<String, String>();

            // ignore number
            st.nextElement();
            entry.put("name", st.nextToken());
            entry.put("url", st.nextToken());
            if (st.hasMoreTokens()) {
                entry.put("picture", st.nextToken());
            }
            context.write(key, WritableUtils.toWritable(entry));
        }
    }

    @Test
    public void testBasicSave() throws Exception {
        Configuration conf = HdpBootstrap.hadoopConfig();
        conf.setBoolean("mapred.used.genericoptionsparser", true);
        conf.set("es.resource", "mrnewapi/save");

        Job job = new Job(conf);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(ESOutputFormat.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setMapperClass(JsonMapper.class);

        TextInputFormat.addInputPath(job, new Path("src/test/resources/artists.dat"));

        job.waitForCompletion(true);
    }
}
