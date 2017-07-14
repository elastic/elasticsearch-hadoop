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
import java.util.Collection;
import java.util.Random;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.HdpBootstrap;
import org.elasticsearch.hadoop.QueryTestParams;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.mr.EsInputFormat;
import org.elasticsearch.hadoop.mr.HadoopCfgUtils;
import org.elasticsearch.hadoop.mr.LinkedMapWritable;
import org.elasticsearch.hadoop.mr.RestUtils;
import org.elasticsearch.hadoop.util.TestSettings;
import org.elasticsearch.hadoop.util.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

@RunWith(Parameterized.class)
public class AbstractMROldApiSearchTest {


    @Parameters
    public static Collection<Object[]> queries() {
        return QueryTestParams.jsonParams();
    }

    private final String query;
    private final String indexPrefix;
    private final Random random = new Random();
    private final boolean readMetadata;
    private final boolean readAsJson;

    public AbstractMROldApiSearchTest(String indexPrefix, String query, boolean readMetadata, boolean readAsJson) {
        this.query = query;
        this.indexPrefix = indexPrefix;
        this.readMetadata = readMetadata;
        this.readAsJson = readAsJson;
    }

    @Before
    public void before() throws Exception {
        RestUtils.refresh(indexPrefix + "mroldapi*");
    }

    @Test
    public void testBasicReadWithConstantRouting() throws Exception {
        String type = "data";
        String target = "mroldapi-savewithconstantrouting/" + type;

        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.ES_MAPPING_ROUTING, "<foobar/>");
        conf.set(ConfigurationOptions.ES_RESOURCE, target);

        JobClient.runJob(conf);
    }

    @Test
    public void testBasicSearch() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, indexPrefix + "mroldapi-save/data");

        JobClient.runJob(conf);
    }


    @Test
    public void testBasicSearchWithWildCard() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, indexPrefix + "mrold*-save/data");

        JobClient.runJob(conf);
    }

    @Test
    public void testSearchWithId() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, indexPrefix + "mroldapi-savewithid/data");

        JobClient.runJob(conf);
    }

    @Test
    public void testSearchNonExistingIndex() throws Exception {
        JobConf conf = createJobConf();
        conf.setBoolean(ConfigurationOptions.ES_INDEX_READ_MISSING_AS_EMPTY, true);
        conf.set(ConfigurationOptions.ES_RESOURCE, "foobar/save");

        JobClient.runJob(conf);
    }

    @Test
    public void testSearchCreated() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, indexPrefix + "mroldapi-createwithid/data");

        JobClient.runJob(conf);
    }

    @Test
    public void testSearchUpdated() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, indexPrefix + "mroldapi-update/data");

        JobClient.runJob(conf);
    }

    @Test(expected = EsHadoopIllegalArgumentException.class)
    public void testSearchUpdatedWithoutUpsertMeaningNonExistingIndex() throws Exception {
        JobConf conf = createJobConf();
        conf.setBoolean(ConfigurationOptions.ES_INDEX_READ_MISSING_AS_EMPTY, false);
        conf.set(ConfigurationOptions.ES_RESOURCE, indexPrefix + "mroldapi-updatewoupsert/data");

        JobClient.runJob(conf);
    }

    @Test
    public void testParentChild() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, indexPrefix + "mroldapi-pc/child");
        conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "no");
        conf.set(ConfigurationOptions.ES_MAPPING_PARENT, "number");

        //conf.set(Stream.class.getName(), "OUT");
        JobClient.runJob(conf);
    }

    @Test
    public void testDynamicPattern() throws Exception {
        Assert.assertTrue(RestUtils.exists("mroldapi-pattern-1/data"));
        Assert.assertTrue(RestUtils.exists("mroldapi-pattern-5/data"));
        Assert.assertTrue(RestUtils.exists("mroldapi-pattern-9/data"));
    }

    @Test
    public void testDynamicPatternWithFormat() throws Exception {
        Assert.assertTrue(RestUtils.exists("mroldapi-pattern-format-2001-10-06/data"));
        Assert.assertTrue(RestUtils.exists("mroldapi-pattern-format-2005-10-06/data"));
        Assert.assertTrue(RestUtils.exists("mroldapi-pattern-format-2017-10-06/data"));
    }

    @Test
    public void testUpsertOnlyParamScriptWithArrayOnArrayField() throws Exception {
        String target = "mroldapi-createwitharrayupsert/data/1";
        Assert.assertTrue(RestUtils.exists(target));
        String result = RestUtils.get(target);
        assertThat(result, not(containsString("ArrayWritable@")));
    }

    //@Test
    public void testNested() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, indexPrefix + "mroldapi-nested/data");
        conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "no");

        //conf.set(Stream.class.getName(), "OUT");
        JobClient.runJob(conf);
    }

    private JobConf createJobConf() throws IOException {
        JobConf conf = HdpBootstrap.hadoopConfig();

        conf.setInputFormat(EsInputFormat.class);
        conf.setOutputFormat(PrintStreamOutputFormat.class);
        conf.setOutputKeyClass(Text.class);
        boolean type = random.nextBoolean();
        Class<?> mapType = (type ? MapWritable.class : LinkedMapWritable.class);
        conf.setOutputValueClass(mapType);
        HadoopCfgUtils.setGenericOptions(conf);
        conf.set(ConfigurationOptions.ES_QUERY, query);
        conf.setNumReduceTasks(0);

        conf.set(ConfigurationOptions.ES_READ_METADATA, String.valueOf(readMetadata));
        conf.set(ConfigurationOptions.ES_READ_METADATA_VERSION, String.valueOf(true));
        conf.set(ConfigurationOptions.ES_OUTPUT_JSON, String.valueOf(readAsJson));

        QueryTestParams.provisionQueries(conf);
        FileInputFormat.setInputPaths(conf, new Path(TestUtils.sampleArtistsDat()));

        HdpBootstrap.addProperties(conf, TestSettings.TESTING_PROPS, false);
        return conf;
    }
}