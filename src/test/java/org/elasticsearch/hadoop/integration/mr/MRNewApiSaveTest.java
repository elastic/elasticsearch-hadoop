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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.integration.HdpBootstrap;
import org.elasticsearch.hadoop.mr.EsOutputFormat;
import org.elasticsearch.hadoop.mr.HadoopCfgUtils;
import org.elasticsearch.hadoop.mr.LinkedMapWritable;
import org.elasticsearch.hadoop.util.RestUtils;
import org.elasticsearch.hadoop.util.WritableUtils;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static org.junit.Assert.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@RunWith(Parameterized.class)
public class MRNewApiSaveTest {

    public static class JsonMapper extends Mapper {

        @Override
        protected void map(Object key, Object value, Context context) throws IOException, InterruptedException {
            StringTokenizer st = new StringTokenizer(value.toString(), "\t");
            Map<String, String> entry = new LinkedHashMap<String, String>();

            entry.put("number", st.nextToken());
            entry.put("name", st.nextToken());
            entry.put("url", st.nextToken());
            if (st.hasMoreTokens()) {
                entry.put("picture", st.nextToken());
            }
            context.write(key, WritableUtils.toWritable(entry));
        }
    }

    @Parameters
    public static Collection<Object[]> configs() throws IOException {
        Configuration conf = HdpBootstrap.hadoopConfig();
        HadoopCfgUtils.setGenericOptions(conf);

        Job job = new Job(conf);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(EsOutputFormat.class);
        job.setMapOutputValueClass(LinkedMapWritable.class);
        job.setMapperClass(JsonMapper.class);
        job.setNumReduceTasks(0);


        Job standard = new Job(job.getConfiguration());
        File fl = new File("src/test/resources/artists.dat");
        long splitSize = fl.length() / 3;
        TextInputFormat.setMaxInputSplitSize(standard, splitSize);
        TextInputFormat.setMinInputSplitSize(standard, 50);

        standard.setMapperClass(JsonMapper.class);
        standard.setMapOutputValueClass(LinkedMapWritable.class);
        TextInputFormat.addInputPath(standard, new Path("src/test/resources/artists.dat"));

        Job json = new Job(job.getConfiguration());
        json.setMapperClass(Mapper.class);
        json.setMapOutputValueClass(Text.class);
        json.getConfiguration().set(ConfigurationOptions.ES_INPUT_JSON, "true");
        TextInputFormat.addInputPath(json, new Path("src/test/resources/artists.json"));

        return Arrays.asList(new Object[][] {
        { standard, "" },
        { json, "json-" } });
    }

    private String indexPrefix = "";
    private Configuration config;

    public MRNewApiSaveTest(Job job, String indexPrefix) {
        this.config = job.getConfiguration();
        this.indexPrefix = indexPrefix;
    }


    @Test
    public void testBasicSave() throws Exception {
        Configuration conf = createConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mrnewapi/save");

        runJob(conf);
    }

    @Test
    public void testSaveWithId() throws Exception {
        Configuration conf = createConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mrnewapi/savewithid");
        conf.set(ConfigurationOptions.ES_MAPPING_ID, "number");

        runJob(conf);
    }

    @Test
    public void testCreateWithId() throws Exception {
        Configuration conf = createConf();
        conf.set(ConfigurationOptions.ES_WRITE_OPERATION, "create");
        conf.set(ConfigurationOptions.ES_MAPPING_ID, "number");
        conf.set(ConfigurationOptions.ES_RESOURCE, "mrnewapi/createwithid");

        runJob(conf);
    }

    @Test
    public void testCreateWithIdShouldFailOnDuplicate() throws Exception {
        Configuration conf = createConf();
        conf.set(ConfigurationOptions.ES_WRITE_OPERATION, "create");
        conf.set(ConfigurationOptions.ES_MAPPING_ID, "number");
        conf.set(ConfigurationOptions.ES_RESOURCE, "mrnewapi/createwithid");

        assertFalse("job should have failed", runJob(conf));
    }

    @Test(expected = EsHadoopIllegalArgumentException.class)
    public void testUpdateWithoutId() throws Exception {
        Configuration conf = createConf();
        conf.set(ConfigurationOptions.ES_WRITE_OPERATION, "update");
        conf.set(ConfigurationOptions.ES_RESOURCE, "mrnewapi/update");

        runJob(conf);
    }

    @Test
    public void testUpdateWithId() throws Exception {
        Configuration conf = createConf();
        conf.set(ConfigurationOptions.ES_WRITE_OPERATION, "update");
        conf.set(ConfigurationOptions.ES_MAPPING_ID, "number");
        conf.set(ConfigurationOptions.ES_RESOURCE, "mrnewapi/update");

        runJob(conf);
    }

    @Test
    public void testUpdateWithoutUpsert() throws Exception {
        Configuration conf = createConf();
        conf.set(ConfigurationOptions.ES_WRITE_OPERATION, "update");
        conf.set(ConfigurationOptions.ES_MAPPING_ID, "number");
        conf.set(ConfigurationOptions.ES_RESOURCE, "mrnewapi/updatewoupsert");
        conf.set(ConfigurationOptions.ES_UPSERT_DOC, "false");

        assertFalse("job should have failed", runJob(conf));
    }

    @Test(expected = EsHadoopIllegalArgumentException.class)
    public void testIndexAutoCreateDisabled() throws Exception {
        Configuration conf = createConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mrnewapi/non-existing");
        conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "no");

        runJob(conf);
    }

    @Test
    public void testParentChild() throws Exception {
        Configuration conf = createConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mrnewapi/child");
        conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "no");
        conf.set(ConfigurationOptions.ES_MAPPING_PARENT, "number");

        RestUtils.putMapping(indexPrefix + "mrnewapi/child", "org/elasticsearch/hadoop/integration/mr-child.json");

        runJob(conf);
    }

    @Test
    public void testIndexPattern() throws Exception {
        Configuration conf = createConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mrnewapi/pattern-{number}");
        conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "yes");

        runJob(conf);
    }

    private Configuration createConf() throws IOException {
        return new Configuration(config);
    }

    private boolean runJob(Configuration conf) throws Exception {
        String string = conf.get(ConfigurationOptions.ES_RESOURCE);
        string = indexPrefix + (string.startsWith("/") ? string.substring(1) : string);
        conf.set(ConfigurationOptions.ES_RESOURCE, string);
        return new Job(conf).waitForCompletion(true);
    }
}