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

import static org.junit.Assume.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@RunWith(Parameterized.class)
public class AbstractMROldApiSaveTest {

    public static class TabMapper extends MapReduceBase implements Mapper {

        @Override
        public void map(Object key, Object value, OutputCollector output, Reporter reporter) throws IOException {
            StringTokenizer st = new StringTokenizer(value.toString(), "\t");
            Map<String, Object> entry = new LinkedHashMap<String, Object>();

            entry.put("number", st.nextToken());
            entry.put("name", st.nextToken());
            entry.put("url", st.nextToken());
            entry.put("list", Arrays.asList("quick", "brown", "fox"));

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
        conf.set(ConfigurationOptions.ES_RESOURCE, "oldapi/multi-save");

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
        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi/constant");
        conf.setMapperClass(ConstantMapper.class);

        runJob(conf);
    }

    @Test
    public void testBasicIndex() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi/save");

        runJob(conf);
    }

    @Test
    public void testBasicIndexWithId() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.ES_MAPPING_ID, "number");
        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi/savewithid");

        runJob(conf);
    }

    @Test
    public void testCreateWithId() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.ES_WRITE_OPERATION, "create");
        conf.set(ConfigurationOptions.ES_MAPPING_ID, "number");
        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi/createwithid");

        runJob(conf);
    }

    //@Test(expected = IOException.class)
    public void testCreateWithIdShouldFailOnDuplicate() throws Exception {
        testCreateWithId();
    }

    @Test(expected = IOException.class)
    public void testUpdateWithoutId() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.ES_WRITE_OPERATION, "upsert");
        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi/update");

        runJob(conf);
    }

    @Test
    public void testUpsertWithId() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.ES_WRITE_OPERATION, "upsert");
        conf.set(ConfigurationOptions.ES_MAPPING_ID, "number");
        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi/update");

        runJob(conf);
    }

    @Test(expected = IOException.class)
    public void testUpdateWithoutUpsert() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.ES_WRITE_OPERATION, "update");
        conf.set(ConfigurationOptions.ES_MAPPING_ID, "number");
        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi/updatewoupsert");

        runJob(conf);
    }

    @Test
    public void testUpdateOnlyScript() throws Exception {
        JobConf conf = createJobConf();
        // use an existing id to allow the update to succeed
        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi/createwithid");
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
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi/createwithid");
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
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi/createwithid");
        conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "yes");

        conf.set(ConfigurationOptions.ES_WRITE_OPERATION, "update");
        conf.set(ConfigurationOptions.ES_MAPPING_ID, "number");
        conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT, "counter = param1; anothercounter = param2");
        conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT_LANG, "groovy");
        conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT_PARAMS_JSON, "{ \"param1\":1, \"param2\":2}");

        runJob(conf);
    }

    @Test
    public void testUpdateOnlyParamJsonScriptWithArray() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi/createwithid");
        conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "yes");

        conf.set(ConfigurationOptions.ES_WRITE_OPERATION, "update");
        conf.set(ConfigurationOptions.ES_MAPPING_ID, "number");
        conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT, "list = new HashSet(); list.add(ctx._source.list); list.add(some_list); ctx._source.list= list.toArray()");
        conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT_LANG, "groovy");
        conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT_PARAMS_JSON, "{ \"some_list\": [\"one\", \"two\"]}");

        runJob(conf);

        //        conf = createJobConf();
        //        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi/createwithid");
        //        conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "yes");
        //
        //        conf.set(ConfigurationOptions.ES_WRITE_OPERATION, "update");
        //        conf.set(ConfigurationOptions.ES_MAPPING_ID, "number");
        //        conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT, "list = new HashSet(); list.add(ctx._source.picture); list.addAll(some_list); ctx._source.picture = list.toArray()");
        //        conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT_LANG, "groovy");
        //        conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT_PARAMS_JSON, "{ \"some_list\": [\"one\", \"two\"]}");
        //
        //        runJob(conf);
    }

    @Test
    public void testUpdateOnlyParamJsonScriptWithArrayOnArrayField() throws Exception {
        String docWithArray = "{ \"counter\" : 1 , \"tags\" : [\"an array\", \"with multiple values\"], \"more_tags\" : [ \"I am tag\"], \"even_more_tags\" : \"I am a tag too\" } ";
        String index = indexPrefix + "mroldapi/createwitharray";
        RestUtils.postData(index + "/1", docWithArray.getBytes());
        RestUtils.refresh(indexPrefix + "mroldapi");
        RestUtils.waitForYellow(indexPrefix + "mroldapi");

        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi/createwitharray");
        conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "yes");

        conf.set(ConfigurationOptions.ES_WRITE_OPERATION, "update");
        conf.set(ConfigurationOptions.ES_MAPPING_ID, "<1>");
        conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT, "tmp = new HashSet(); tmp.addAll(ctx._source.tags); tmp.addAll(new_date); ctx._source.tags = tmp.toArray()");
        conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT_LANG, "groovy");
        conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT_PARAMS_JSON, "{ \"new_date\": [\"add me\", \"and me\"]}");

        runJob(conf);
    }


    @Test
    public void testUpsertScript() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi/upsert-script");
        conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "yes");
        conf.set(ConfigurationOptions.ES_WRITE_OPERATION, "upsert");
        conf.set(ConfigurationOptions.ES_MAPPING_ID, "number");
        conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT, "counter = 1");

        runJob(conf);
    }

    @Test
    public void testUpsertParamScript() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi/upsert-script-param");
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
        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi/upsert-script-json-param");
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
        String index = indexPrefix + "mroldapi/createwitharrayupsert";
        RestUtils.postData(index + "/1", docWithArray.getBytes());
        RestUtils.refresh(indexPrefix + "mroldapi");
        RestUtils.waitForYellow(indexPrefix + "mroldapi");

        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi/createwitharrayupsert");
        conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "yes");

        conf.set(ConfigurationOptions.ES_WRITE_OPERATION, "upsert");
        conf.set(ConfigurationOptions.ES_MAPPING_ID, "<1>");
        conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT, "ctx._source.tags = update_tags");
        conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT_LANG, "groovy");
        conf.set(ConfigurationOptions.ES_UPDATE_SCRIPT_PARAMS, (conf.get(ConfigurationOptions.ES_INPUT_JSON).equals("true") ? "update_tags:name" :"update_tags:list"));

        runJob(conf);
    }


    @Test(expected = EsHadoopIllegalArgumentException.class)
    public void testIndexAutoCreateDisabled() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi/non-existing");
        conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "no");

        runJob(conf);
    }

    @Test
    public void testIndexWithVersionMappingImpliesVersionTypeExternal() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi/external-version-implied");
        conf.set(ConfigurationOptions.ES_MAPPING_VERSION, "number");

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
        RestUtils.putMapping(indexPrefix + "mroldapi/child", "org/elasticsearch/hadoop/integration/mr-child.json");
        RestUtils.putMapping(indexPrefix + "mroldapi/parent", StringUtils.toUTF("{\"parent\":{}}"));

        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi/mr-parent");
        runJob(conf);

        conf = createJobConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi/child");
        conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "no");
        conf.set(ConfigurationOptions.ES_MAPPING_PARENT, "number");

        runJob(conf);
    }

    @Test
    public void testIndexPattern() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "/mroldapi/pattern-{number}");
        conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "yes");

        runJob(conf);
    }

    @Test
    public void testIndexPatternWithFormatting() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi/pattern-format-{@timestamp:YYYY-MM-dd}");
        conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "yes");

        runJob(conf);
    }

    @Test
    public void testIndexPatternWithFormattingAndId() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi/pattern-format-{@timestamp:YYYY-MM-dd}-with-id");
        conf.set(ConfigurationOptions.ES_MAPPING_ID, "number");

        runJob(conf);
    }

    @Test
    public void testIndexWithEscapedJson() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi/simple-escaped-fields");
        conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "yes");

        runJob(conf);
    }


    //@Test
    public void testNested() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, "mroldapi/nested");
        conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "no");

        RestUtils.putMapping(indexPrefix + "mroldapi/nested", "org/elasticsearch/hadoop/integration/mr-nested.json");

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