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
package org.elasticsearch.hadoop.hive;

import org.elasticsearch.hadoop.util.TestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class HiveLoaderTest {

    private static HiveEmbeddedServer server;

    {
        TestUtils.hackHadoopStagingOnWin();
        TestUtils.assertElasticsearchIsRunning();
    }

    @BeforeClass
    public static void start() throws Exception {
        server = new HiveEmbeddedServer();
        server.start();
    }

    @AfterClass
    public static void stop() throws Exception {
        server.stop();
    }

    @Test
    public void basicSave() throws Exception {
        String create = "CREATE EXTERNAL TABLE artists ("
                + "id 		BIGINT, "
                + "name 	STRING, "
                + "links 	STRUCT<url:STRING, picture:STRING>) "
                + "STORED BY 'org.elasticsearch.hadoop.hive.ESStorageHandler' "
                + "TBLPROPERTIES('es.resource' = 'radio/artists/_search?q=me*') ";

        String select = "SELECT * FROM artists";

        System.out.println(server.execute(create));
        System.out.println(server.execute(select));
    }
}
