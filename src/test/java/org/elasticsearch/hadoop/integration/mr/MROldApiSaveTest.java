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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.integration.HdpBootstrap;
import org.elasticsearch.hadoop.mr.ESOutputFormat;
import org.elasticsearch.hadoop.util.WritableUtils;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class MROldApiSaveTest {

    public static class JsonMapper extends MapReduceBase implements Mapper {

        @Override
        public void map(Object key, Object value, OutputCollector output, Reporter reporter) throws IOException {
            StringTokenizer st = new StringTokenizer(value.toString(), "\t");
            Map<String, String> entry = new LinkedHashMap<String, String>();

            entry.put("number", st.nextToken());
            entry.put("name", st.nextToken());
            entry.put("url", st.nextToken());
            if (st.hasMoreTokens()) {
                entry.put("picture", st.nextToken());
            }

            output.collect(key, WritableUtils.toWritable(entry));
        }
    }

    @Test
    public void testBasicIndex() throws Exception {
        JobConf conf = HdpBootstrap.hadoopConfig();

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(ESOutputFormat.class);
        conf.setMapOutputValueClass(MapWritable.class);
        conf.setMapperClass(JsonMapper.class);
        conf.setReducerClass(IdentityReducer.class);
        conf.setBoolean("mapred.used.genericoptionsparser", true);

        FileInputFormat.setInputPaths(conf, new Path("src/test/resources/artists.dat"));
        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi/save");

        JobClient.runJob(conf);
    }

    @Test
    public void testBasicIndexWithId() throws Exception {
        JobConf conf = HdpBootstrap.hadoopConfig();

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(ESOutputFormat.class);
        conf.setMapOutputValueClass(MapWritable.class);
        conf.setMapperClass(JsonMapper.class);
        conf.setReducerClass(IdentityReducer.class);
        conf.setBoolean("mapred.used.genericoptionsparser", true);
        conf.set(ConfigurationOptions.ES_MAPPING_ID, "number");
        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi/savewithid");

        FileInputFormat.setInputPaths(conf, new Path("src/test/resources/artists.dat"));

        JobClient.runJob(conf);
    }

    @Test
    public void testCreateWithId() throws Exception {
        JobConf conf = HdpBootstrap.hadoopConfig();

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(ESOutputFormat.class);
        conf.setMapOutputValueClass(MapWritable.class);
        conf.setMapperClass(JsonMapper.class);
        conf.setReducerClass(IdentityReducer.class);
        conf.setBoolean("mapred.used.genericoptionsparser", true);

        FileInputFormat.setInputPaths(conf, new Path("src/test/resources/artists.dat"));

        conf.set(ConfigurationOptions.ES_OPERATION, "create");
        conf.set(ConfigurationOptions.ES_MAPPING_ID, "number");
        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi/createwithid");

        JobClient.runJob(conf);
    }

    @Test(expected = IOException.class)
    public void testCreateWithIdShouldFailOnDuplicate() throws Exception {
        JobConf conf = HdpBootstrap.hadoopConfig();

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(ESOutputFormat.class);
        conf.setMapOutputValueClass(MapWritable.class);
        conf.setMapperClass(JsonMapper.class);
        conf.setReducerClass(IdentityReducer.class);
        conf.setBoolean("mapred.used.genericoptionsparser", true);

        FileInputFormat.setInputPaths(conf, new Path("src/test/resources/artists.dat"));

        conf.set(ConfigurationOptions.ES_OPERATION, "create");
        conf.set(ConfigurationOptions.ES_MAPPING_ID, "number");
        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi/createwithid");

        JobClient.runJob(conf);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUpdateWithoutId() throws Exception {
        JobConf conf = HdpBootstrap.hadoopConfig();

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(ESOutputFormat.class);
        conf.setMapOutputValueClass(MapWritable.class);
        conf.setMapperClass(JsonMapper.class);
        conf.setReducerClass(IdentityReducer.class);
        conf.setBoolean("mapred.used.genericoptionsparser", true);

        FileInputFormat.setInputPaths(conf, new Path("src/test/resources/artists.dat"));

        conf.set(ConfigurationOptions.ES_OPERATION, "update");
        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi/update");

        JobClient.runJob(conf);
    }

    @Test
    public void testUpdateWithId() throws Exception {
        JobConf conf = HdpBootstrap.hadoopConfig();

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(ESOutputFormat.class);
        conf.setMapOutputValueClass(MapWritable.class);
        conf.setMapperClass(JsonMapper.class);
        conf.setReducerClass(IdentityReducer.class);
        conf.setBoolean("mapred.used.genericoptionsparser", true);

        FileInputFormat.setInputPaths(conf, new Path("src/test/resources/artists.dat"));

        conf.set(ConfigurationOptions.ES_OPERATION, "update");
        conf.set(ConfigurationOptions.ES_MAPPING_ID, "number");
        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi/update");

        JobClient.runJob(conf);
    }

    @Test(expected = IOException.class)
    public void testUpdateWithoutUpsert() throws Exception {
        JobConf conf = HdpBootstrap.hadoopConfig();

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(ESOutputFormat.class);
        conf.setMapOutputValueClass(MapWritable.class);
        conf.setMapperClass(JsonMapper.class);
        conf.setReducerClass(IdentityReducer.class);
        conf.setBoolean("mapred.used.genericoptionsparser", true);

        FileInputFormat.setInputPaths(conf, new Path("src/test/resources/artists.dat"));

        conf.set(ConfigurationOptions.ES_OPERATION, "update");
        conf.set(ConfigurationOptions.ES_MAPPING_ID, "number");
        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi/updatewoupsert");
        conf.set(ConfigurationOptions.ES_UPSERT_DOC, "false");

        JobClient.runJob(conf);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIndexAutoCreateDisabled() throws Exception {
        JobConf conf = HdpBootstrap.hadoopConfig();

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(ESOutputFormat.class);
        conf.setMapOutputValueClass(MapWritable.class);
        conf.setMapperClass(JsonMapper.class);
        conf.setReducerClass(IdentityReducer.class);
        conf.setBoolean("mapred.used.genericoptionsparser", true);

        FileInputFormat.setInputPaths(conf, new Path("src/test/resources/artists.dat"));
        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi/non-existing");
        conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "no");

        JobClient.runJob(conf);
    }
}
