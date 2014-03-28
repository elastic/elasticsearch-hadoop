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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.integration.HdpBootstrap;
import org.elasticsearch.hadoop.integration.QueryTestParams;
import org.elasticsearch.hadoop.mr.EsInputFormat;
import org.elasticsearch.hadoop.mr.HadoopCfgUtils;
import org.elasticsearch.hadoop.mr.LinkedMapWritable;
import org.elasticsearch.hadoop.util.RestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class MRNewApiSearchTest {

    @Parameters
    public static Collection<Object[]> queries() {
        return QueryTestParams.jsonParams();
    }

    private final String query;
    private final String indexPrefix;

    public MRNewApiSearchTest(String indexPrefix, String query) {
        this.indexPrefix = indexPrefix;
        this.query = query;
    }

    @Test
    public void testBasicSearch() throws Exception {
        Configuration conf = createConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, indexPrefix + "mrnewapi/save");

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


    private Configuration createConf() throws IOException {
        Configuration conf = HdpBootstrap.hadoopConfig();
        HadoopCfgUtils.setGenericOptions(conf);
        Job job = new Job(conf);
        job.setInputFormatClass(EsInputFormat.class);
        job.setOutputFormatClass(PrintStreamOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LinkedMapWritable.class);
        conf.set(ConfigurationOptions.ES_QUERY, query);

        DistributedCache.addFileToClassPath(new Path("src/test/resources/org/elasticsearch/hadoop/integration/query.dsl"), conf);
        DistributedCache.addFileToClassPath(new Path("src/test/resources/org/elasticsearch/hadoop/integration/query.uri"), conf);

        job.setNumReduceTasks(0);
        //PrintStreamOutputFormat.stream(conf, Stream.OUT);
        return job.getConfiguration();
    }
}