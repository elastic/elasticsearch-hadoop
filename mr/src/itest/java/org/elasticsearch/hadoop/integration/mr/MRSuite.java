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

import org.elasticsearch.hadoop.HdfsUtils;
import org.elasticsearch.hadoop.LocalEs;
import org.elasticsearch.hadoop.util.TestUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.ExternalResource;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({ AbstractMROldApiSaveTest.class, AbstractMROldApiSearchTest.class, AbstractMRNewApiSaveTest.class, AbstractMRNewApiSearchTest.class, AbstractExtraMRTests.class })
//@Suite.SuiteClasses({ AbstractMROldApiSaveTest.class, AbstractMROldApiSearchTest.class })
//@Suite.SuiteClasses({ AbstractMROldApiSaveTest.class })
public class MRSuite {
    @ClassRule
    public static ExternalResource resource = new LocalEs();

    @BeforeClass
    public static void setupHdfs() throws Exception {
        HdfsUtils.copyFromLocal(TestUtils.sampleArtistsDat());
        HdfsUtils.copyFromLocal(TestUtils.sampleArtistsJson());
    }
}