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
package org.elasticsearch.hadoop.integration.hive;

import org.elasticsearch.hadoop.integration.LocalES;
import org.elasticsearch.hadoop.integration.TestSettings;
import org.junit.ClassRule;
import org.junit.rules.ChainedExternalResource;
import org.junit.rules.ExternalResource;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({ HiveSaveTest.class, HiveSearchTest.class })
//@Suite.SuiteClasses({ HiveSearchTest.class })
public class HiveSuite {

    static HiveEmbeddedServer server;
    static String cleanDdl = "DROP DATABASE IF EXISTS test CASCADE";
    static String createDB = "CREATE DATABASE test";
    static String useDB = "USE test";

    public static ExternalResource hive = new ExternalResource() {
        @Override
        protected void before() throws Throwable {
            server = new HiveEmbeddedServer(TestSettings.TESTING_PROPS);
            server.start();

            server.execute(cleanDdl);
            server.execute(createDB);
            server.execute(useDB);
        }

        @Override
        protected void after() {
            try {
                server.execute(cleanDdl);
            } catch (Exception ex) {
            }
            server.stop();
        }
    };

    @ClassRule
    public static ExternalResource resource = new ChainedExternalResource(new LocalES(), hive);
}
