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
import org.elasticsearch.hadoop.util.EsMajorVersion;
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
import static org.junit.Assume.assumeTrue;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@RunWith(Parameterized.class)
public class AbstractMRNewApiSaveTest {

    private EsMajorVersion version = TestUtils.getEsVersion();

    public static class TabMapper extends Mapper {

        @Override
        protected void map(Object key, Object value, Context context) throws IOException, InterruptedException {
            StringTokenizer st = new StringTokenizer(value.toString(), "\t");
            Map<String, String> entry = new LinkedHashMap<String, String>();

            entry.put("number", st.nextToken());
            entry.put("name", st.nextToken());
            entry.put("url", st.nextToken());

            while (st.hasMoreTokens()) {
                String str = st.nextToken();
                if (str.startsWith("http")) {
                    entry.put("picture", str);
                } else if (str.startsWith("20")) {
                    entry.put("@timestamp", str);
                } else if (str.startsWith("1") || str.startsWith("2") || str.startsWith("5") || str.startsWith("9") || str.startsWith("10")) {
                    entry.put("tag", str);
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
        conf.set(ConfigurationOptions.ES_RESOURCE, "mrnewapi-multi-save/data");

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
        conf.set(ConfigurationOptions.ES_RESOURCE, "mrnewapi-save/data");

        runJob(conf);
    }

    @Test
    public void testSaveWithId() throws Exception {
        Configuration conf = createConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mrnewapi-savewithid/data");
        conf.set(ConfigurationOptions.ES_MAPPING_ID, "number");

        runJob(conf);
    }

    @Test
    public void testCreateWithId() throws Exception {
        Configuration conf = createConf();
        conf.set(ConfigurationOptions.ES_WRITE_OPERATION, "create");
        conf.set(ConfigurationOptions.ES_MAPPING_ID, "number");
        conf.set(ConfigurationOptions.ES_RESOURCE, "mrnewapi-createwithid/data");

        runJob(conf);
    }

    @Test
    public void testCreateWithIdShouldFailOnDuplicate() throws Exception {
        Configuration conf = createConf();
        conf.set(ConfigurationOptions.ES_WRITE_OPERATION, "create");
        conf.set(ConfigurationOptions.ES_MAPPING_ID, "number");
        conf.set(ConfigurationOptions.ES_RESOURCE, "mrnewapi-createwithid/data");

        assertFalse("job should have failed", runJob(conf));
    }

    @Test
    public void testSaveWithIngest() throws Exception {
        RestUtils.ExtendedRestClient versionTestingClient = new RestUtils.ExtendedRestClient();
        EsMajorVersion esMajorVersion = versionTestingClient.remoteEsVersion();
        assumeTrue("Ingest Supported in 5.x and above only", esMajorVersion.onOrAfter(EsMajorVersion.V_5_X));
        versionTestingClient.close();

        Configuration conf = createConf();

        RestUtils.ExtendedRestClient client = new RestUtils.ExtendedRestClient();
        String prefix = "mrnewapi";
        String pipeline = "{\"description\":\"Test Pipeline\",\"processors\":[{\"set\":{\"field\":\"pipeTEST\",\"value\":true,\"override\":true}}]}";
        client.put("/_ingest/pipeline/" + prefix + "-pipeline", StringUtils.toUTF(pipeline));
        client.close();

        conf.set(ConfigurationOptions.ES_RESOURCE, "mrnewapi-ingested/data");
        conf.set(ConfigurationOptions.ES_INGEST_PIPELINE, "mrnewapi-pipeline");
        conf.set(ConfigurationOptions.ES_NODES_INGEST_ONLY, "true");

        runJob(conf);
    }

    @Test(expected = EsHadoopIllegalArgumentException.class)
    public void testUpdateWithoutId() throws Exception {
        Configuration conf = createConf();
        conf.set(ConfigurationOptions.ES_WRITE_OPERATION, "update");
        conf.set(ConfigurationOptions.ES_RESOURCE, "mrnewapi-update/data");

        runJob(conf);
    }

    @Test
    public void testUpsertWithId() throws Exception {
        Configuration conf = createConf();
        conf.set(ConfigurationOptions.ES_WRITE_OPERATION, "upsert");
        conf.set(ConfigurationOptions.ES_MAPPING_ID, "number");
        conf.set(ConfigurationOptions.ES_RESOURCE, "mrnewapi-update/data");

        runJob(conf);
    }

    @Test
    public void testUpdateWithoutUpsert() throws Exception {
        Configuration conf = createConf();
        conf.set(ConfigurationOptions.ES_WRITE_OPERATION, "update");
        conf.set(ConfigurationOptions.ES_MAPPING_ID, "number");
        conf.set(ConfigurationOptions.ES_RESOURCE, "mrnewapi-updatewoupsert/data");

        assertFalse("job should have failed", runJob(conf));
    }

    @Test
    public void testUpdateOnlyScript() throws Exception {
        Configuration conf = createConf();
        // use an existing id to allow the update to succeed
        conf.set(ConfigurationOptions.ES_RESOURCE, "mrnewapi-createwithid/data");
        conf.set(ConfigurationOptions.ES_WRITE_OPERATION, "update");
        conf.set(ConfigurationOptions.ES_MAPPING_ID, "number");

        conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "yes");
        conf.set(ConfigurationOptions.ES_UPDATE_RETRY_ON_CONFLICT, "3");

        if (version.onOrAfter(EsMajorVersion.V_5_X)) {
            conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT, "int counter = 3");
            conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT_LANG, "painless");
        } else {
            conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT, "counter = 3");
            conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT_LANG, "groovy");
        }

        runJob(conf);
    }

    @Test
    public void testUpdateOnlyParamScript() throws Exception {
        Configuration conf = createConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mrnewapi-createwithid/data");
        conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "yes");

        conf.set(ConfigurationOptions.ES_WRITE_OPERATION, "update");
        conf.set(ConfigurationOptions.ES_MAPPING_ID, "number");
        conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT_PARAMS, " param1:<1>,   param2:number ");

        if (version.onOrAfter(EsMajorVersion.V_5_X)) {
            conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT_LANG, "painless");
            conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT, "int counter = params.param1; String anothercounter = params.param2");
        } else {
            conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT_LANG, "groovy");
            conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT, "counter = param1; anothercounter = param2");
        }

        runJob(conf);
    }

    @Test
    public void testUpdateOnlyParamJsonScript() throws Exception {
        Configuration conf = createConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mrnewapi-createwithid/data");
        conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "yes");

        conf.set(ConfigurationOptions.ES_WRITE_OPERATION, "update");
        conf.set(ConfigurationOptions.ES_MAPPING_ID, "number");
        conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT_PARAMS_JSON, "{ \"param1\":1, \"param2\":2}");

        if (version.onOrAfter(EsMajorVersion.V_5_X)) {
            conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT, "int counter = params.param1; int anothercounter = params.param2");
            conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT_LANG, "painless");
        } else {
            conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT, "counter = param1; anothercounter = param2");
            conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT_LANG, "groovy");
        }

        runJob(conf);
    }

    @Test
    public void testUpsertScript() throws Exception {
        Configuration conf = createConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mrnewapi-upsert-script/data");
        conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "yes");
        conf.set(ConfigurationOptions.ES_WRITE_OPERATION, "upsert");
        conf.set(ConfigurationOptions.ES_MAPPING_ID, "number");
        conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT, "counter = 1");

        runJob(conf);
    }

    @Test
    public void testUpsertParamScript() throws Exception {
        Configuration conf = createConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mrnewapi-upsert-script-param/data");
        conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "yes");
        conf.set(ConfigurationOptions.ES_WRITE_OPERATION, "upsert");
        conf.set(ConfigurationOptions.ES_MAPPING_ID, "number");
        conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT_PARAMS, " param1:<1>,   param2:number ");

        if (version.onOrAfter(EsMajorVersion.V_5_X)) {
            conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT, "int counter = params.param1; int anothercounter = Integer.parseInt(params.param2)");
            conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT_LANG, "painless");
        } else {
            conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT, "counter += param1; anothercounter += param2");
            conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT_LANG, "groovy");
        }

        runJob(conf);
    }

    @Test
    public void testUpsertParamJsonScript() throws Exception {
        Configuration conf = createConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mrnewapi-upsert-script-param/data");
        conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "yes");
        conf.set(ConfigurationOptions.ES_WRITE_OPERATION, "upsert");
        conf.set(ConfigurationOptions.ES_MAPPING_ID, "number");
        conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT_PARAMS_JSON, "{ \"param1\":1, \"param2\":2}");

        if (version.onOrAfter(EsMajorVersion.V_5_X)) {
            conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT, "int counter = params.param1; int anothercounter = params.param2");
            conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT_LANG, "painless");
        } else {
            conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT, "counter += param1; anothercounter += param2");
            conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT_LANG, "groovy");
        }

        runJob(conf);
    }

    @Test(expected = EsHadoopIllegalArgumentException.class)
    public void testIndexAutoCreateDisabled() throws Exception {
        Configuration conf = createConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mrnewapi-non-existing/data");
        conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "no");

        runJob(conf);
    }

    @Test
    public void testParentChild() throws Exception {
        // in ES 2.x, the parent/child relationship needs to be created fresh
        // hence why we reindex everything again
        RestUtils.putMapping(indexPrefix + "mrnewapi-pc/child", "org/elasticsearch/hadoop/integration/mr-child.json");
        RestUtils.putMapping(indexPrefix + "mrnewapi-pc/parent", StringUtils.toUTF("{\"parent\":{}}"));

        Configuration conf = createConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mrnewapi-pc/parent");
        runJob(conf);

        conf = createConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mrnewapi-pc/child");
        conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "no");
        conf.set(ConfigurationOptions.ES_MAPPING_PARENT, "number");

        runJob(conf);
    }

    @Test
    public void testIndexPattern() throws Exception {
        Configuration conf = createConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mrnewapi-pattern-{tag}/data");
        conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "yes");

        runJob(conf);
    }

    @Test
    public void testIndexPatternWithFormatting() throws Exception {
        Configuration conf = createConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mrnewapi-pattern-format-{@timestamp|YYYY-MM-dd}/data");
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