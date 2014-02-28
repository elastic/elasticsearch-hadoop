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
package org.elasticsearch.hadoop.integration.cascading;

import org.apache.hadoop.mapred.JobConf;
import org.elasticsearch.hadoop.integration.HdfsUtils;
import org.elasticsearch.hadoop.integration.HdpBootstrap;
import org.elasticsearch.hadoop.integration.LocalEs;
import org.elasticsearch.hadoop.integration.Provisioner;
import org.elasticsearch.hadoop.integration.QueryTestParams;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.ExternalResource;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import cascading.util.Update;

@RunWith(Suite.class)
@Suite.SuiteClasses({ CascadingHadoopSaveTest.class, CascadingHadoopSearchTest.class })
//@Suite.SuiteClasses({ CascadingHadoopSearchTest.class })
public class CascadingHadoopSuite {

    @ClassRule
    public static ExternalResource resource = new LocalEs();

    public static JobConf configuration;

    @BeforeClass
    public static void setup() throws Exception {
        configuration = HdpBootstrap.hadoopConfig();
        System.setProperty(Update.UPDATE_CHECK_SKIP, "true");

        //configuration.setNumReduceTasks(0);
        if (!HdpBootstrap.isJtLocal(configuration)) {
            configuration = Provisioner.cascading(configuration);
        }
        HdfsUtils.copyFromLocal("src/test/resources/artists.dat");
        HdfsUtils.copyFromLocal("src/test/resources/org/elasticsearch/hadoop/integration/query.dsl");
        HdfsUtils.copyFromLocal("src/test/resources/org/elasticsearch/hadoop/integration/query.uri");

        QueryTestParams.provisionQueries(configuration);
    }
}
