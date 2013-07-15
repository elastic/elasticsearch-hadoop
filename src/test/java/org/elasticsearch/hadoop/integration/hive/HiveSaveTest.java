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

import org.elasticsearch.hadoop.integration.HdfsUtils;
import org.junit.Before;
import org.junit.Test;

import static org.elasticsearch.hadoop.integration.hive.HiveSuite.*;

public class HiveSaveTest {


    @Before
    public void provision() {
        // provision on each test run since LOAD DATA _moves_ the file
        if (!isLocal) {
            hdfsResource = "/eshdp/hive/hive-compund.dat";
            HdfsUtils.copyFromLocal(originalResource, hdfsResource);
        }
    }

    @Test
    public void testBasicSave() throws Exception {
        // load the raw data as a native, managed table
        // and then insert its content into the external one

        String jar = "ADD JAR /tmp/es-hadoop.jar";
        String localTable = "CREATE TABLE source ("
                + "id       BIGINT, "
                + "name     STRING, "
                + "url      STRING, "
                + "picture  STRING) "
                + "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'"
                + "LOCATION '/tmp/hive/warehouse/source/' ";

        String load = "LOAD DATA INPATH '" + HiveSuite.hdfsResource + "' OVERWRITE INTO TABLE source";

        // create external table
        String ddl =
                "CREATE EXTERNAL TABLE artistssave ("
                + "id       BIGINT, "
                + "name     STRING, "
                + "links    STRUCT<url:STRING, picture:STRING>) "
                + "STORED BY 'org.elasticsearch.hadoop.hive.ESStorageHandler' "
                + "WITH SERDEPROPERTIES ('serder.foo' = 'serder.bar') "
                + "TBLPROPERTIES('es.resource' = 'hive/artists') ";

        String selectTest = "SELECT NULL, s.name, struct(s.url, s.picture) FROM source s";

        // transfer data
        String insert =
                "INSERT OVERWRITE TABLE artistssave "
                + "SELECT NULL, s.name, named_struct('url', s.url, 'picture', s.picture) FROM source s";

        System.out.println(server.execute(jar));
        System.out.println(server.execute(ddl));
        System.out.println(server.execute(localTable));
        System.out.println(server.execute(load));
        System.out.println(server.execute(selectTest));
        System.out.println(server.execute(insert));
    }

    @Test
    // see http://shmsoft.blogspot.ro/2011/10/loading-inner-maps-in-hive.html
    public void testCompoundSave() throws Exception {

        String jar = "ADD JAR /tmp/es-hadoop.jar";

        // load the raw data as a native, managed table
        // and then insert its content into the external one
        String localTable = "CREATE TABLE compoundsource ("
                + "rid      INT, "
                + "mapids   ARRAY<INT>, "
                + "rdata    MAP<INT, STRING>)"
                + "ROW FORMAT DELIMITED "
                + "FIELDS TERMINATED BY '\t' "
                + "COLLECTION ITEMS TERMINATED BY ',' "
                + "MAP KEYS TERMINATED BY ':' "
                + "LINES TERMINATED BY '\n' "
                + "LOCATION '/tmp/hive/warehouse/compound/' ";


        // load the data - use the URI just to be sure
        String uri = HiveSuite.hdfsResource;
        String load = "LOAD DATA INPATH '" + uri + "' OVERWRITE INTO TABLE compoundSource";


        // create external table
        String ddl =
                "CREATE EXTERNAL TABLE compoundsave ("
                + "rid      INT, "
                + "mapids   ARRAY<INT>, "
                + "rdata    MAP<INT, STRING>) "
                + "STORED BY 'org.elasticsearch.hadoop.hive.ESStorageHandler' "
                + "TBLPROPERTIES('es.resource' = 'hive/compound') ";

        String selectTest = "SELECT rid, mapids, rdata FROM compoundsource";

        // transfer data
        String insert =
                "INSERT OVERWRITE TABLE compoundsave "
                + "SELECT rid, mapids, rdata FROM compoundsource";

        System.out.println(server.execute(jar));
        System.out.println(server.execute(localTable));
        System.out.println(server.execute(load));
        System.out.println(server.execute(selectTest));
        System.out.println(server.execute(ddl));
        System.out.println(server.execute(insert));
    }
}