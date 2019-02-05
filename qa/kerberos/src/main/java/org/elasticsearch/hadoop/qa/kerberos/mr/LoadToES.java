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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.elasticsearch.hadoop.mr.EsMapReduceUtil;
import org.elasticsearch.hadoop.mr.EsOutputFormat;
import org.elasticsearch.hadoop.mr.LinkedMapWritable;
import org.elasticsearch.hadoop.qa.kerberos.security.KeytabLogin;

public class LoadToES extends Configured implements Tool {

    public static final String CONF_FIELD_NAMES = "load.field.names";

    public static void main(final String[] args) throws Exception {
        KeytabLogin.doAfterLogin(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
                System.exit(ToolRunner.run(new LoadToES(), args));
                return null;
            }
        });
    }

    @Override
    public int run(String[] args) throws Exception {
        if (getConf().get(CONF_FIELD_NAMES, null) == null) {
            throw new IllegalArgumentException("Must include configuration '" + CONF_FIELD_NAMES + "'");
        }

        Job job = Job.getInstance(getConf(), "LoadToES");
        // DO NOT SET JAR BY CLASS HERE
        //
        // job.setJarByClass(getClass());

        EsMapReduceUtil.initCredentials(job);

        TextInputFormat.addInputPath(job, new Path(args[0]));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(EsOutputFormat.class);

        job.setMapperClass(MapperImpl.class);
        // Secure Hadoop CANNOT perform shuffle phases without native libraries
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(LinkedMapWritable.class);

        if (!job.waitForCompletion(true)) {
            return 1;
        }
        return 0;
    }

    public static class MapperImpl extends Mapper<LongWritable, Text, NullWritable, LinkedMapWritable> {

        List<Text> fieldNames = new ArrayList<Text>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            for (String fieldName : context.getConfiguration().getStrings(CONF_FIELD_NAMES)) {
                fieldNames.add(new Text(fieldName));
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            LinkedMapWritable record = new LinkedMapWritable();
            String line = value.toString();
            Iterator<Text> fieldNameIter = fieldNames.iterator();
            for (StringTokenizer tokenizer = new StringTokenizer(line, "\t"); tokenizer.hasMoreTokens(); ) {
                if (fieldNameIter.hasNext()) {
                    Text fieldName = fieldNameIter.next();
                    String field = tokenizer.nextToken();
                    record.put(fieldName, new Text(field));
                }
            }
            context.write(NullWritable.get(), record);
        }
    }
}
