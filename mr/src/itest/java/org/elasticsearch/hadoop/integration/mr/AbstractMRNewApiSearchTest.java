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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.HdpBootstrap;
import org.elasticsearch.hadoop.QueryTestParams;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.mr.EsInputFormat;
import org.elasticsearch.hadoop.mr.HadoopCfgUtils;
import org.elasticsearch.hadoop.mr.LinkedMapWritable;
import org.elasticsearch.hadoop.mr.RestUtils;
import org.elasticsearch.hadoop.util.TestSettings;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class AbstractMRNewApiSearchTest {

    @Parameters
    public static Collection<Object[]> queries() {
        return QueryTestParams.jsonParams();
    }

    private final String query;
    private final String indexPrefix;
    private final Random random = new Random();
    private final boolean readMetadata;
    private final boolean readAsJson;

    public AbstractMRNewApiSearchTest(String indexPrefix, String query, boolean readMetadata, boolean readAsJson) {
        this.indexPrefix = indexPrefix;
        this.query = query;
        this.readMetadata = readMetadata;
        this.readAsJson = readAsJson;
    }

    @Before
    public void before() throws Exception {
        RestUtils.refresh(indexPrefix + "mrnewapi");
    }

    @Test
    public void testBasicSearch() throws Exception {
        Configuration conf = createConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, indexPrefix + "mrnewapi/save");

        new Job(conf).waitForCompletion(true);
    }

    @Test
    public void testBasicWildSearch() throws Exception {
        Configuration conf = createConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, indexPrefix + "mrnew*/save");

        new Job(conf).waitForCompletion(true);
    }

    @Test
    public void testSearchWithId() throws Exception {
        Configuration conf = createConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, indexPrefix + "mrnewapi/savewithid");

        new Job(conf).waitForCompletion(true);
    }

    @Test
    public void testSearchNonExistingIndex() throws Exception {
        Configuration conf = createConf();
        conf.setBoolean(ConfigurationOptions.ES_INDEX_READ_MISSING_AS_EMPTY, true);
        conf.set(ConfigurationOptions.ES_RESOURCE, indexPrefix + "foobar/save");

        new Job(conf).waitForCompletion(true);
    }

    @Test
    public void testSearchCreated() throws Exception {
        Configuration conf = createConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, indexPrefix + "mrnewapi/createwithid");

        new Job(conf).waitForCompletion(true);
    }

    @Test
    public void testSearchUpdated() throws Exception {
        Configuration conf = createConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, indexPrefix + "mrnewapi/update");

        new Job(conf).waitForCompletion(true);
    }

    @Test(expected = EsHadoopIllegalArgumentException.class)
    public void testSearchUpdatedWithoutUpsertMeaningNonExistingIndex() throws Exception {
        Configuration conf = createConf();
        conf.setBoolean(ConfigurationOptions.ES_INDEX_READ_MISSING_AS_EMPTY, false);
        conf.set(ConfigurationOptions.ES_RESOURCE, indexPrefix + "mrnewapi/updatewoupsert");

        new Job(conf).waitForCompletion(true);
    }

    @Test
    public void testParentChild() throws Exception {
        Configuration conf = createConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, indexPrefix + "mrnewapi/child");
        conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "no");
        conf.set(ConfigurationOptions.ES_MAPPING_PARENT, "number");

        //conf.set(Stream.class.getName(), "OUT");
        new Job(conf).waitForCompletion(true);
    }

    @Test
    public void testDynamicPattern() throws Exception {
        Assert.assertTrue(RestUtils.exists("mrnewapi/pattern-1"));
        Assert.assertTrue(RestUtils.exists("mrnewapi/pattern-500"));
        Assert.assertTrue(RestUtils.exists("mrnewapi/pattern-990"));
    }

    @Test
    public void testDynamicPatternWithFormat() throws Exception {
        Assert.assertTrue(RestUtils.exists("mrnewapi/pattern-format-2936-10-06"));
        Assert.assertTrue(RestUtils.exists("mrnewapi/pattern-format-2051-10-06"));
        Assert.assertTrue(RestUtils.exists("mrnewapi/pattern-format-2945-10-06"));
    }


    private Configuration createConf() throws IOException {
        Configuration conf = HdpBootstrap.hadoopConfig();
        HadoopCfgUtils.setGenericOptions(conf);
        Job job = new Job(conf);
        job.setInputFormatClass(EsInputFormat.class);
        job.setOutputFormatClass(PrintStreamOutputFormat.class);
        job.setOutputKeyClass(Text.class);

        boolean type = random.nextBoolean();
        Class<?> mapType = (type ? MapWritable.class : LinkedMapWritable.class);

        job.setOutputValueClass(mapType);
        conf.set(ConfigurationOptions.ES_QUERY, query);

        conf.set(ConfigurationOptions.ES_READ_METADATA, String.valueOf(readMetadata));
        conf.set(ConfigurationOptions.ES_OUTPUT_JSON, String.valueOf(readAsJson));

        QueryTestParams.provisionQueries(conf);
        job.setNumReduceTasks(0);
        //PrintStreamOutputFormat.stream(conf, Stream.OUT);

        Configuration cfg = job.getConfiguration();
        HdpBootstrap.addProperties(cfg, TestSettings.TESTING_PROPS, false);
        return cfg;
    }
}