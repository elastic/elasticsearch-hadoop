/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticsearch.hadoop.integration.mr;

import org.elasticsearch.hadoop.integration.HdfsUtils;
import org.elasticsearch.hadoop.integration.LocalES;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.ExternalResource;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
//@Suite.SuiteClasses({ MROldApiSaveTest.class, MROldApiSearchTest.class , MRNewApiSaveTest.class, MRNewApiSearchTest.class })
@Suite.SuiteClasses({ MROldApiSaveTest.class, MROldApiSearchTest.class })
public class MRSuite {
    @ClassRule
    public static ExternalResource resource = new LocalES();

    @BeforeClass
    public static void setupHdfs() throws Exception {
        HdfsUtils.copyFromLocal("src/test/resources/artists.dat");
    }
}
