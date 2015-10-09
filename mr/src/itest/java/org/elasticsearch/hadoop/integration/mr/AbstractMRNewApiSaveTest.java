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
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.HdpBootstrap;
import org.elasticsearch.hadoop.Stream;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.mr.EsOutputFormat;
import org.elasticsearch.hadoop.mr.HadoopCfgUtils;
import org.elasticsearch.hadoop.mr.LinkedMapWritable;
import org.elasticsearch.hadoop.mr.MultiOutputFormat;
import org.elasticsearch.hadoop.mr.RestUtils;
import org.elasticsearch.hadoop.util.StringUtils;
import org.elasticsearch.hadoop.util.TestSettings;
import org.elasticsearch.hadoop.util.TestUtils;
import org.elasticsearch.hadoop.util.WritableUtils;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static org.junit.Assert.assertFalse;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@RunWith(Parameterized.class)
public class AbstractMRNewApiSaveTest {

    public static class TabMapper extends Mapper {

        @Override
        protected void map(Object key, Object value, Context context) throws IOException, InterruptedException {
            StringTokenizer st = new StringTokenizer(value.toString(), "\t");
            Map<String, String> entry = new LinkedHashMap<String, String>();

            entry.put("number", st.nextToken());
            entry.put("name", st.nextToken());
            entry.put("url", st.nextToken());

            if (st.hasMoreTokens()) {
                String str = st.nextToken();
                if (str.startsWith("http")) {
                    entry.put("picture", str);

                    if (st.hasMoreTokens()) {
                        String token = st.nextToken();
                        entry.put("@timestamp", token);
                    }
                }
                else {
                    entry.put("@timestamp", str);
                }
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
        job.setMapperClass(TabMapper.class);
        job.setNumReduceTasks(0);


        Job standard = new Job(job.getConfiguration());
        File fl = new File(TestUtils.sampleArtistsDat());
        long splitSize = fl.length() / 3;
        TextInputFormat.setMaxInputSplitSize(standard, splitSize);
        TextInputFormat.setMinInputSplitSize(standard, 50);

        standard.setMapperClass(TabMapper.class);
        standard.setMapOutputValueClass(LinkedMapWritable.class);
        TextInputFormat.addInputPath(standard, new Path(TestUtils.sampleArtistsDat(conf)));

        Job json = new Job(job.getConfiguration());
        json.setMapperClass(Mapper.class);
        json.setMapOutputValueClass(Text.class);
        json.getConfiguration().set(ConfigurationOptions.ES_INPUT_JSON, "true");
        TextInputFormat.addInputPath(json, new Path(TestUtils.sampleArtistsJson(conf)));

        return Arrays.asList(new Object[][] {
                { standard, "" },
                { json, "json-" } });
    }

    private String indexPrefix = "";
    private final Configuration config;

    public AbstractMRNewApiSaveTest(Job job, String indexPrefix) {
        this.config = job.getConfiguration();
        this.indexPrefix = indexPrefix;

        HdpBootstrap.addProperties(config, TestSettings.TESTING_PROPS, false);
    }

    @Test
    public void testBasicMultiSave() throws Exception {
        Configuration conf = createConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mrnewapi/multi-save");

        MultiOutputFormat.addOutputFormat(conf, EsOutputFormat.class);
        MultiOutputFormat.addOutputFormat(conf, PrintStreamOutputFormat.class);
        //MultiOutputFormat.addOutputFormat(conf, TextOutputFormat.class);

        PrintStreamOutputFormat.stream(conf, Stream.OUT);
        //conf.set("mapred.output.dir", "foo/bar");

        conf.setClass("mapreduce.outputformat.class", MultiOutputFormat.class, OutputFormat.class);
        runJob(conf);
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
    public void testUpsertWithId() throws Exception {
        Configuration conf = createConf();
        conf.set(ConfigurationOptions.ES_WRITE_OPERATION, "upsert");
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

        assertFalse("job should have failed", runJob(conf));
    }

    @Test
    public void testUpdateOnlyScript() throws Exception {
        Configuration conf = createConf();
        // use an existing id to allow the update to succeed
        conf.set(ConfigurationOptions.ES_RESOURCE, "mrnewapi/createwithid");
        conf.set(ConfigurationOptions.ES_WRITE_OPERATION, "update");
        conf.set(ConfigurationOptions.ES_MAPPING_ID, "number");

        conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "yes");
        conf.set(ConfigurationOptions.ES_UPDATE_RETRY_ON_CONFLICT, "3");
        conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT, "counter = 3");
        conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT_LANG, "groovy");

        runJob(conf);
    }

    @Test
    public void testUpdateOnlyParamScript() throws Exception {
        Configuration conf = createConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mrnewapi/createwithid");
        conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "yes");

        conf.set(ConfigurationOptions.ES_WRITE_OPERATION, "update");
        conf.set(ConfigurationOptions.ES_MAPPING_ID, "number");
        conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT, "counter = param1; anothercounter = param2");
        conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT_LANG, "groovy");
        conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT_PARAMS, " param1:<1>,   param2:number ");

        runJob(conf);
    }

    @Test
    public void testUpdateOnlyParamJsonScript() throws Exception {
        Configuration conf = createConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mrnewapi/createwithid");
        conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "yes");

        conf.set(ConfigurationOptions.ES_WRITE_OPERATION, "update");
        conf.set(ConfigurationOptions.ES_MAPPING_ID, "number");
        conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT, "counter = param1; anothercounter = param2");
        conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT_LANG, "groovy");
        conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT_PARAMS_JSON, "{ \"param1\":1, \"param2\":2}");

        runJob(conf);
    }

    @Test
    public void testUpsertScript() throws Exception {
        Configuration conf = createConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mrnewapi/upsert-script");
        conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "yes");
        conf.set(ConfigurationOptions.ES_WRITE_OPERATION, "upsert");
        conf.set(ConfigurationOptions.ES_MAPPING_ID, "number");
        conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT, "counter = 1");

        runJob(conf);
    }

    @Test
    public void testUpsertParamScript() throws Exception {
        Configuration conf = createConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mrnewapi/upsert-script-param");
        conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "yes");
        conf.set(ConfigurationOptions.ES_WRITE_OPERATION, "upsert");
        conf.set(ConfigurationOptions.ES_MAPPING_ID, "number");
        conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT, "counter += param1; anothercounter += param2");
        conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT_LANG, "groovy");
        conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT_PARAMS, " param1:<1>,   param2:number ");

        runJob(conf);
    }

    @Test
    public void testUpsertParamJsonScript() throws Exception {
        Configuration conf = createConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mrnewapi/upsert-script-param");
        conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "yes");
        conf.set(ConfigurationOptions.ES_WRITE_OPERATION, "upsert");
        conf.set(ConfigurationOptions.ES_MAPPING_ID, "number");
        conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT, "counter += param1; anothercounter += param2");
        conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT_LANG, "groovy");
        conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT_PARAMS_JSON, "{ \"param1\":1, \"param2\":2}");

        runJob(conf);
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
        // in ES 2.x, the parent/child relationship needs to be created fresh
        // hence why we reindex everything again

        String childIndex = indexPrefix + "child";
        String parentIndex = indexPrefix + "mr_parent";

        //String mapping = "{ \"" + parentIndex + "\" : {}, \"" + childIndex + "\" : { \"_parent\" : { \"type\" : \"" + parentIndex + "\" }}}";
        //RestUtils.putMapping(indexPrefix + "mroldapi/child", StringUtils.toUTF(mapping));
        RestUtils.putMapping(indexPrefix + "mrnewapi/child", "org/elasticsearch/hadoop/integration/mr-child.json");
        RestUtils.putMapping(indexPrefix + "mrnewapi/parent", StringUtils.toUTF("{\"parent\":{}}"));

        Configuration conf = createConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mrnewapi/mr-parent");
        runJob(conf);

        conf = createConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mrnewapi/child");
        conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "no");
        conf.set(ConfigurationOptions.ES_MAPPING_PARENT, "number");

        runJob(conf);
    }

    @Test
    public void testIndexPattern() throws Exception {
        Configuration conf = createConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mrnewapi/pattern-{number}");
        conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "yes");

        runJob(conf);
    }

    @Test
    public void testIndexPatternWithFormatting() throws Exception {
        Configuration conf = createConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mrnewapi/pattern-format-{@timestamp:YYYY-MM-dd}");
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