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
package org.elasticsearch.hadoop.integration.pig;

import org.elasticsearch.hadoop.HdfsUtils;
import org.elasticsearch.hadoop.LocalEs;
import org.elasticsearch.hadoop.Provisioner;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.ExternalResource;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({ AbstractPigSaveTest.class, AbstractPigSaveJsonTest.class, AbstractPigSearchTest.class, AbstractPigSearchJsonTest.class })
//@Suite.SuiteClasses({ PigSaveTest.class })
public class PigSuite {

    @ClassRule
    public static ExternalResource resource = new LocalEs();

    @BeforeClass
    public static void setup() {
        HdfsUtils.copyFromLocal(Provisioner.ESHADOOP_TESTING_JAR, Provisioner.HDFS_ES_HDP_LIB);
    }
}
