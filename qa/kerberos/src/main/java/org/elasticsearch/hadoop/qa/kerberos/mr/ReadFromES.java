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

package org.elasticsearch.hadoop.qa.kerberos.mr;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.elasticsearch.hadoop.mr.EsInputFormat;
import org.elasticsearch.hadoop.mr.EsMapReduceUtil;
import org.elasticsearch.hadoop.qa.kerberos.security.KeytabLogin;

public class ReadFromES extends Configured implements Tool {

    public static void main(final String[] args) throws Exception {
        KeytabLogin.doAfterLogin(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
                System.exit(ToolRunner.run(new ReadFromES(), args));
                return null;
            }
        });
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "ReadFromES");
        // DO NOT SET JAR BY CLASS HERE
        //
        // job.setJarByClass(getClass());

        EsMapReduceUtil.initCredentials(job);

        job.getConfiguration().set("es.output.json", "true");

        job.setInputFormatClass(EsInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextOutputFormat.setOutputPath(job, new Path(args[0]));

        job.setMapperClass(MapperImpl.class);
        // Secure Hadoop CANNOT perform shuffle phases without native libraries
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        if (!job.waitForCompletion(true)) {
            return 1;
        }
        return 0;
    }

    public static class MapperImpl extends Mapper<Text, Text, NullWritable, Text> {

        private final NullWritable nullKey = NullWritable.get();

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(nullKey, value);
        }
    }
}
