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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.integration.HdpBootstrap;
import org.elasticsearch.hadoop.integration.QueryTestParams;
import org.elasticsearch.hadoop.mr.EsInputFormat;
import org.elasticsearch.hadoop.mr.HadoopCfgUtils;
import org.elasticsearch.hadoop.mr.LinkedMapWritable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class MROldApiSearchTest {

    @Parameters
    public static Collection<Object[]> queries() {
        return QueryTestParams.jsonParams();
    }

    private final String query;
    private final String indexPrefix;

    public MROldApiSearchTest(String indexPrefix, String query) {
        this.query = query;
        this.indexPrefix = indexPrefix;
    }

    @Test
    public void testBasicSearch() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, indexPrefix + "mroldapi/save");

        JobClient.runJob(conf);
    }

    @Test
    public void testSearchWithId() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, indexPrefix + "mroldapi/savewithid");

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
        conf.set(ConfigurationOptions.ES_RESOURCE, indexPrefix + "mroldapi/createwithid");

        JobClient.runJob(conf);
    }

    @Test
    public void testSearchUpdated() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, indexPrefix + "mroldapi/update");

        JobClient.runJob(conf);
    }

    @Test(expected = EsHadoopIllegalArgumentException.class)
    public void testSearchUpdatedWithoutUpsertMeaningNonExistingIndex() throws Exception {
        JobConf conf = createJobConf();
        conf.setBoolean(ConfigurationOptions.ES_INDEX_READ_MISSING_AS_EMPTY, false);
        conf.set(ConfigurationOptions.ES_RESOURCE, indexPrefix + "mroldapi/updatewoupsert");

        JobClient.runJob(conf);
    }

    @Test
    public void testParentChild() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, indexPrefix + "mroldapi/child");
        conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "no");
        conf.set(ConfigurationOptions.ES_MAPPING_PARENT, "number");

        //conf.set(Stream.class.getName(), "OUT");
        JobClient.runJob(conf);
    }

    //@Test
    public void testNested() throws Exception {
        JobConf conf = createJobConf();
        conf.set(ConfigurationOptions.ES_RESOURCE, indexPrefix + "mroldapi/nested");
        conf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "no");

        //conf.set(Stream.class.getName(), "OUT");
        JobClient.runJob(conf);
    }

    private JobConf createJobConf() throws IOException {
        JobConf conf = HdpBootstrap.hadoopConfig();

        conf.setInputFormat(EsInputFormat.class);
        conf.setOutputFormat(PrintStreamOutputFormat.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(LinkedMapWritable.class);
        HadoopCfgUtils.setGenericOptions(conf);
        conf.set(ConfigurationOptions.ES_QUERY, query);
        conf.setNumReduceTasks(0);

        QueryTestParams.provisionQueries(conf);
        FileInputFormat.setInputPaths(conf, new Path("src/test/resources/artists.dat"));
        return conf;
    }
}