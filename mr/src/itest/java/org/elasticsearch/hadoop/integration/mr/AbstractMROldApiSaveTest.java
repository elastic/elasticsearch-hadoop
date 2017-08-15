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
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapreduce.OutputFormat;
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
import org.junit.Assume;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static org.junit.Assume.assumeFalse;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@RunWith(Parameterized.class)
public class AbstractMROldApiSaveTest {

    private EsMajorVersion version = TestUtils.getEsVersion();

    public static class TabMapper extends MapReduceBase implements Mapper {

        @Override
        public void map(Object key, Object value, OutputCollector output, Reporter reporter) throws IOException {
            StringTokenizer st = new StringTokenizer(value.toString(), "\t");
            Map<String, Object> entry = new LinkedHashMap<String, Object>();

            entry.put("number", st.nextToken());
            entry.put("name", st.nextToken());
            entry.put("url", st.nextToken());
            entry.put("list", Arrays.asList("quick", "brown", "fox"));

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

            output.collect(key, WritableUtils.toWritable(entry));
        }
    }

    public static class ConstantMapper extends MapReduceBase implements Mapper {

        @Override
        public void map(Object key, Object value, OutputCollector output, Reporter reporter) throws IOException {
            MapWritable map = new MapWritable();
            map.put(new Text("key"), new Text("value"));
            output.collect(new LongWritable(), map);
        }
    }


    public static class SplittableTextInputFormat extends TextInputFormat {

        @Override
        public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
            return super.getSplits(job, job.getInt("actual.splits", 3));
        }
    }

    @Parameters
    public static Collection<Object[]> configs() {
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
        FileInputFormat.setInputPaths(standard, new Path(TestUtils.sampleArtistsDat(conf)));

        JobConf json = new JobConf(conf);
        json.setMapperClass(IdentityMapper.class);
        json.setMapOutputValueClass(Text.class);
        json.set(ConfigurationOptions.ES_INPUT_JSON, "true");
        FileInputFormat.setInputPaths(json, new Path(TestUtils.sampleArtistsJson(conf)));

        return Arrays.asList(new Object[][] {
                { standard, "" },
                { json, "json-" }
        });
    }

    private String indexPrefix = "";
    private final JobConf config;

    public AbstractMROldApiSaveTest(JobConf config, String indexPrefix) {
        this.indexPrefix = indexPrefix;
        this.config = config;

        HdpBootstrap.addProperties(config, TestSettings.TESTING_PROPS, false);
    }

    @Test
    public void testBasicMultiSave() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "oldapi-multi-save/data");

        MultiOutputFormat.addOutputFormat(conf, EsOutputFormat.class);
        MultiOutputFormat.addOutputFormat(conf, PrintStreamOutputFormat.class);
        //MultiOutputFormat.addOutputFormat(conf, TextOutputFormat.class);

        PrintStreamOutputFormat.stream(conf, Stream.OUT);
        //conf.set("mapred.output.dir", "foo/bar");
        //FileOutputFormat.setOutputPath(conf, new Path("foo/bar"));

        conf.setClass("mapred.output.format.class", MultiOutputFormat.class, OutputFormat.class);
        runJob(conf);
    }


    @Test
    public void testNoInput() throws Exception {
        JobConf conf = createJobConf();

        // use only when dealing with constant input
        assumeFalse(conf.get(ConfigurationOptions.ES_INPUT_JSON).equals("true"));
        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi-constant/data");
        conf.setMapperClass(ConstantMapper.class);

        runJob(conf);
    }

    @Test
    public void testBasicIndex() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi-save/data");

        runJob(conf);
    }

    @Test
    public void testBasicIndexWithId() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.ES_MAPPING_ID, "number");
        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi-savewithid/data");

        runJob(conf);
    }

    @Test
    public void testBasicIndexWithExtractedRouting() throws Exception {
        String type = "data";
        String target = "mroldapi-savewithdynamicrouting/" + type;

        RestUtils.touch(indexPrefix + "mroldapi-savewithdynamicrouting");
        RestUtils.putMapping(indexPrefix + target, StringUtils.toUTF("{\""+ type + "\":{\"_routing\": {\"required\":true}}}"));

        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.ES_MAPPING_ROUTING, "number");
        conf.set(ConfigurationOptions.ES_RESOURCE, target);

        runJob(conf);
    }

    @Test
    public void testBasicIndexWithConstantRouting() throws Exception {
        String type = "data";
        String target = "mroldapi-savewithconstantrouting/" + type;

        RestUtils.touch(indexPrefix + "mroldapi-savewithconstantrouting");
        RestUtils.putMapping(indexPrefix + target, StringUtils.toUTF("{\""+ type + "\":{\"_routing\": {\"required\":true}}}"));

        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.ES_MAPPING_ROUTING, "<foobar/>");
        conf.set(ConfigurationOptions.ES_RESOURCE, target);

        runJob(conf);
    }

    @Test
    public void testCreateWithId() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.ES_WRITE_OPERATION, "create");
        conf.set(ConfigurationOptions.ES_MAPPING_ID, "number");
        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi-createwithid/data");

        runJob(conf);
    }

    //@Test(expected = IOException.class)
    public void testCreateWithIdShouldFailOnDuplicate() throws Exception {
        testCreateWithId();
    }

    @Test
    public void testSaveWithIngest() throws Exception {
        RestUtils.ExtendedRestClient versionTestingClient = new RestUtils.ExtendedRestClient();
        EsMajorVersion esMajorVersion = versionTestingClient.remoteEsVersion();
        Assume.assumeTrue("Ingest Supported in 5.x and above only", esMajorVersion.onOrAfter(EsMajorVersion.V_5_X));
        versionTestingClient.close();

        JobConf conf = createJobConf();

        RestUtils.ExtendedRestClient client = new RestUtils.ExtendedRestClient();
        String prefix = "mroldapi";
        String pipeline = "{\"description\":\"Test Pipeline\",\"processors\":[{\"set\":{\"field\":\"pipeTEST\",\"value\":true,\"override\":true}}]}";
        client.put("/_ingest/pipeline/" + prefix + "-pipeline", StringUtils.toUTF(pipeline));
        client.close();

        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi-ingested/data");
        conf.set(ConfigurationOptions.ES_INGEST_PIPELINE, "mroldapi-pipeline");
        conf.set(ConfigurationOptions.ES_NODES_INGEST_ONLY, "true");

        runJob(conf);
    }


    @Test(expected = IOException.class)
    public void testUpdateWithoutId() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.ES_WRITE_OPERATION, "upsert");
        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi-update/data");

        runJob(conf);
    }

    @Test
    public void testUpsertWithId() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.ES_WRITE_OPERATION, "upsert");
        conf.set(ConfigurationOptions.ES_MAPPING_ID, "number");
        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi-update/data");

        runJob(conf);
    }

    @Test(expected = IOException.class)
    public void testUpdateWithoutUpsert() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.ES_WRITE_OPERATION, "update");
        conf.set(ConfigurationOptions.ES_MAPPING_ID, "number");
        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi-updatewoupsert/data");

        runJob(conf);
    }

    @Test
    public void testUpdateOnlyScript() throws Exception {
        JobConf conf = createJobConf();
        // use an existing id to allow the update to succeed
        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi-createwithid/data");
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
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi-createwithid/data");
        conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "yes");

        conf.set(ConfigurationOptions.ES_WRITE_OPERATION, "update");
        conf.set(ConfigurationOptions.ES_MAPPING_ID, "number");
        conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT_PARAMS, " param1:<1>,   param2:number ");

        if (version.onOrAfter(EsMajorVersion.V_5_X)) {
            conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT, "int counter = params.param1; String anothercounter = params.param2");
            conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT_LANG, "painless");
        } else {
            conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT, "counter = param1; anothercounter = param2");
            conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT_LANG, "groovy");
        }

        runJob(conf);
    }

    @Test
    public void testUpdateOnlyParamJsonScript() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi-createwithid/data");
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
    public void testUpdateOnlyParamJsonScriptWithArray() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi-createwithid/data");
        conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "yes");

        conf.set(ConfigurationOptions.ES_WRITE_OPERATION, "update");
        conf.set(ConfigurationOptions.ES_MAPPING_ID, "number");
        conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT_PARAMS_JSON, "{ \"some_list\": [\"one\", \"two\"]}");

        if (version.onOrAfter(EsMajorVersion.V_5_X)) {
            conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT, "HashSet list = new HashSet(); list.add(ctx._source.list); list.add(params.some_list); ctx._source.list = list.toArray()");
            conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT_LANG, "painless");
        } else {
            conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT, "list = new HashSet(); list.add(ctx._source.list); list.add(some_list); ctx._source.list= list.toArray()");
            conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT_LANG, "groovy");
        }

        runJob(conf);
    }

    @Test
    public void testUpdateOnlyParamJsonScriptWithArrayOnArrayField() throws Exception {
        String docWithArray = "{ \"counter\" : 1 , \"tags\" : [\"an array\", \"with multiple values\"], \"more_tags\" : [ \"I am tag\"], \"even_more_tags\" : \"I am a tag too\" } ";
        String index = indexPrefix + "mroldapi-createwitharray/data";
        RestUtils.postData(index + "/1", docWithArray.getBytes());
        RestUtils.refresh(indexPrefix + "mroldapi-createwitharray");
        RestUtils.waitForYellow(indexPrefix + "mroldapi-createwitharray");

        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi-createwitharray/data");
        conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "yes");

        conf.set(ConfigurationOptions.ES_WRITE_OPERATION, "update");
        conf.set(ConfigurationOptions.ES_MAPPING_ID, "<1>");
        conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT_PARAMS_JSON, "{ \"new_date\": [\"add me\", \"and me\"]}");

        if (version.onOrAfter(EsMajorVersion.V_5_X)) {
            conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT, "HashSet tmp = new HashSet(); tmp.addAll(ctx._source.tags); tmp.addAll(params.new_date); ctx._source.tags = tmp.toArray()");
            conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT_LANG, "painless");
        } else {
            conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT, "tmp = new HashSet(); tmp.addAll(ctx._source.tags); tmp.addAll(new_date); ctx._source.tags = tmp.toArray()");
            conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT_LANG, "groovy");
        }

        runJob(conf);
    }


    @Test
    public void testUpsertScript() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi-upsert-script/data");
        conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "yes");
        conf.set(ConfigurationOptions.ES_WRITE_OPERATION, "upsert");
        conf.set(ConfigurationOptions.ES_MAPPING_ID, "number");
        conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT, "counter = 1");

        runJob(conf);
    }

    @Test
    public void testUpsertParamScript() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi-upsert-script-param/data");
        conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "yes");
        conf.set(ConfigurationOptions.ES_WRITE_OPERATION, "upsert");
        conf.set(ConfigurationOptions.ES_MAPPING_ID, "number");
        conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT, "counter += param1; anothercounter += param2");
        conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT_LANG, "groovy");
        conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT_PARAMS, "param2:name , param3:number, param1:<1>");

        runJob(conf);
    }

    @Test
    public void testUpsertParamJsonScript() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi-upsert-script-json-param/data");
        conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "yes");
        conf.set(ConfigurationOptions.ES_WRITE_OPERATION, "upsert");
        conf.set(ConfigurationOptions.ES_MAPPING_ID, "number");
        conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT, "counter += param1; anothercounter += param2");
        conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT_LANG, "groovy");
        conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT_PARAMS_JSON, "{ \"param1\":1, \"param2\":2}");

        runJob(conf);
    }

    @Test
    public void testUpsertOnlyParamScriptWithArrayOnArrayField() throws Exception {
        String docWithArray = "{ \"counter\" : 1 , \"tags\" : [\"an array\", \"with multiple values\"], \"more_tags\" : [ \"I am tag\"], \"even_more_tags\" : \"I am a tag too\" } ";
        String index = indexPrefix + "mroldapi-createwitharrayupsert/data";
        RestUtils.postData(index + "/1", docWithArray.getBytes());
        RestUtils.refresh(indexPrefix + "mroldapi-createwitharrayupsert");
        RestUtils.waitForYellow(indexPrefix + "mroldapi-createwitharrayupsert");

        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi-createwitharrayupsert/data");
        conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "yes");

        conf.set(ConfigurationOptions.ES_WRITE_OPERATION, "upsert");
        conf.set(ConfigurationOptions.ES_MAPPING_ID, "<1>");
        conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT_PARAMS, (conf.get(ConfigurationOptions.ES_INPUT_JSON).equals("true") ? "update_tags:name" :"update_tags:list"));

        if (version.onOrAfter(EsMajorVersion.V_5_X)) {
            conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT, "ctx._source.tags = params.update_tags");
            conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT_LANG, "painless");
        } else {
            conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT, "ctx._source.tags = update_tags");
            conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT_LANG, "groovy");
        }

        runJob(conf);
    }


    @Test(expected = EsHadoopIllegalArgumentException.class)
    public void testIndexAutoCreateDisabled() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi-non-existing/data");
        conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "no");

        runJob(conf);
    }

    @Test
    public void testIndexWithVersionMappingImpliesVersionTypeExternal() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi-external-version-implied/data");
        // an id must be provided if version type or value are set
        conf.set(ConfigurationOptions.ES_MAPPING_ID, "number");
        conf.set(ConfigurationOptions.ES_MAPPING_VERSION, "number");

        runJob(conf);
    }

    @Test
    public void testParentChild() throws Exception {
        // in ES 2.x, the parent/child relationship needs to be created fresh
        // hence why we reindex everything again

        String index = indexPrefix + "mroldapi-pc";
        String parentResource = index + "/parent";
        String childResource = index + "/child";

        System.out.println(indexPrefix + "mroldapi-pc");
        System.out.println(parentResource);
        System.out.println(childResource);

        RestUtils.putMapping(childResource, "org/elasticsearch/hadoop/integration/mr-child.json");
        RestUtils.putMapping(parentResource, StringUtils.toUTF("{\"parent\":{}}"));

        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi-pc/parent");
        runJob(conf);

        conf = createJobConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi-pc/child");
        conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "no");
        conf.set(ConfigurationOptions.ES_MAPPING_PARENT, "number");

        runJob(conf);
    }

    @Test
    public void testIndexPattern() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi-pattern-{tag}/data");
        conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "yes");

        runJob(conf);
    }

    @Test
    public void testIndexPatternWithFormatting() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi-pattern-format-{@timestamp|YYYY-MM-dd}/data");
        conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "yes");

        runJob(conf);
    }

    @Test
    public void testIndexPatternWithFormattingAndId() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi-pattern-format-{@timestamp|YYYY-MM-dd}-with-id/data");
        conf.set(ConfigurationOptions.ES_MAPPING_ID, "number");

        runJob(conf);
    }

    @Test
    public void testIndexWithEscapedJson() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi-simple-escaped-fields/data");
        conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "yes");

        runJob(conf);
    }


    //@Test
    public void testNested() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi-nested/data");
        conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "no");

        RestUtils.putMapping(indexPrefix + "mroldapi-nested/data", "org/elasticsearch/hadoop/integration/mr-nested.json");

        runJob(conf);
    }

    private JobConf createJobConf() {
        return new JobConf(config);
    }

    private void runJob(JobConf conf) throws Exception {
        String string = conf.get(ConfigurationOptions.ES_RESOURCE);
        string = indexPrefix + (string.startsWith("/") ? string.substring(1) : string);
        conf.set(ConfigurationOptions.ES_RESOURCE, string);
        JobClient.runJob(conf);
    }
}