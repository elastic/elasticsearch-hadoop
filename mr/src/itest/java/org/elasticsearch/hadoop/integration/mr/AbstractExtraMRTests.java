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
package org.elasticsearch.hadoop.integration.mr;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.elasticsearch.hadoop.HdpBootstrap;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.integration.mr.AbstractMROldApiSaveTest.SplittableTextInputFormat;
import org.elasticsearch.hadoop.mr.EsInputFormat;
import org.elasticsearch.hadoop.mr.EsOutputFormat;
import org.elasticsearch.hadoop.mr.HadoopCfgUtils;
import org.elasticsearch.hadoop.mr.LinkedMapWritable;
import org.elasticsearch.hadoop.mr.RestUtils;
import org.elasticsearch.hadoop.util.TestSettings;
import org.elasticsearch.hadoop.util.TestUtils;
import org.elasticsearch.hadoop.util.WritableUtils;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@RunWith(Parameterized.class)
public class AbstractExtraMRTests {

    private final Random random = new Random();

    public static class TabMapper extends MapReduceBase implements Mapper {

        @Override
        public void map(Object key, Object value, OutputCollector output, Reporter reporter) throws IOException {
            StringTokenizer st = new StringTokenizer(value.toString(), "\t");
            Map<String, Object> entry = new LinkedHashMap<String, Object>();

            entry.put("@id", st.nextToken());
            entry.put("@key", st.nextToken());
            entry.put("@timestamp", st.nextToken());
            entry.put("@value", st.nextToken());

            output.collect(key, WritableUtils.toWritable(entry));
        }
    }

    @Parameters
    public static Collection<Object[]> configs() throws IOException {
        JobConf conf = HdpBootstrap.hadoopConfig();

        conf.setInputFormat(SplittableTextInputFormat.class);
        conf.setOutputFormat(EsOutputFormat.class);
        conf.setReducerClass(IdentityReducer.class);
        HadoopCfgUtils.setGenericOptions(conf);
        conf.setNumMapTasks(2);
        conf.setInt("actual.splits", 2);
        conf.setNumReduceTasks(0);


        JobConf standard = new JobConf(conf);
        standard.setMapperClass(TabMapper.class);
        standard.setMapOutputValueClass(LinkedMapWritable.class);
        standard.set(ConfigurationOptions.ES_INPUT_JSON, "false");
        FileInputFormat.setInputPaths(standard, new Path(TestUtils.gibberishDat(conf)));

        JobConf json = new JobConf(conf);
        json.setMapperClass(IdentityMapper.class);
        json.setMapOutputValueClass(Text.class);
        json.set(ConfigurationOptions.ES_INPUT_JSON, "true");
        FileInputFormat.setInputPaths(json, new Path(TestUtils.gibberishJson(conf)));

        return Arrays.asList(new Object[][] { { standard, "" }, { json, "json-" } });
    }

    private String indexPrefix = "";
    private final JobConf config;

    public AbstractExtraMRTests(JobConf config, String indexPrefix) {
        this.indexPrefix = indexPrefix;
        this.config = config;

        HdpBootstrap.addProperties(config, TestSettings.TESTING_PROPS, false);
    }


    @Test
    public void testSaveDocWithEscapedChars() throws Exception {
        JobConf conf = new JobConf(config);
        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi-gibberish/data");
        runJob(conf);
    }

    @Test
    public void testSaveDocWithEscapedCharsAndMapping() throws Exception {
        JobConf conf = new JobConf(config);
        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi-gibberish-with-mapping/data");
        conf.set(ConfigurationOptions.ES_MAPPING_ID, "@id");
        runJob(conf);
    }

    @Test
    public void testXLoadDoc() throws Exception {
        JobConf conf = createReadJobConf();

        conf.set(ConfigurationOptions.ES_RESOURCE, indexPrefix + "mroldapi-gibberish/data");
        JobClient.runJob(conf);
    }

    @Test
    public void testXLoadDocWithMapping() throws Exception {
        JobConf conf = createReadJobConf();

        conf.set(ConfigurationOptions.ES_RESOURCE, indexPrefix + "mroldapi-gibberish-with-mapping/data");
        JobClient.runJob(conf);
    }

    @Test
    public void testIndexAlias() throws Exception {
        String simpleDoc = "{ \"number\" : 1 , \"list\" : [\"an array\", \"with multiple values\"], \"song\" : \"Three Headed Guardian\" } ";
        String targetPrefix = indexPrefix + "index";
        String alias = indexPrefix + "alias";
        String targetA = targetPrefix + "a/type";
        String targetB = targetPrefix + "b/type";
        RestUtils.postData(targetA + "/1", simpleDoc.getBytes());
        RestUtils.postData(targetB + "/1", simpleDoc.getBytes());

        // put alias
        String aliases =
                "{ \"actions\" : [ " +
                        "{ \"add\":{\"index\":\"" + targetPrefix + "a\",\"alias\":\"" + alias + "\" }} ," +
                        "{ \"add\":{\"index\":\"" + targetPrefix + "b\",\"alias\":\"" + alias + "\" }}  " +
                        "]}";

        RestUtils.postData("_aliases", aliases.getBytes());
        RestUtils.refresh(alias);

        // run MR job
        JobConf conf = createReadJobConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, indexPrefix + "alias/type");
        JobClient.runJob(conf);
    }

    private void runJob(JobConf conf) throws Exception {
        String string = conf.get(ConfigurationOptions.ES_RESOURCE);
        string = indexPrefix + (string.startsWith("/") ? string.substring(1) : string);
        conf.set(ConfigurationOptions.ES_RESOURCE, string);
        JobClient.runJob(conf);
    }

    private JobConf createReadJobConf() throws IOException {
        JobConf conf = HdpBootstrap.hadoopConfig();

        conf.setInputFormat(EsInputFormat.class);
        conf.setOutputFormat(PrintStreamOutputFormat.class);
        conf.setOutputKeyClass(Text.class);
        boolean type = random.nextBoolean();
        Class<?> mapType = (type ? MapWritable.class : LinkedMapWritable.class);
        conf.setOutputValueClass(MapWritable.class);
        HadoopCfgUtils.setGenericOptions(conf);
        conf.setNumReduceTasks(0);

        conf.set(ConfigurationOptions.ES_READ_METADATA, String.valueOf(random.nextBoolean()));
        conf.set(ConfigurationOptions.ES_READ_METADATA_VERSION, String.valueOf(true));
        conf.set(ConfigurationOptions.ES_OUTPUT_JSON, "true");

        FileInputFormat.setInputPaths(conf, new Path(TestUtils.gibberishDat(conf)));
        return conf;
    }
}