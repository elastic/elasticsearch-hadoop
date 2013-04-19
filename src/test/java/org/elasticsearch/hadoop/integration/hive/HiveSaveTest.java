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

import org.junit.Test;

import static org.elasticsearch.hadoop.integration.hive.HiveSuite.*;

public class HiveSaveTest {

    @Test
    public void basicSave() throws Exception {

        // load the raw data as a native, managed table
        // and then insert its content into the external one

        String localTable = "CREATE TABLE source ("
                + "id       BIGINT, "
                + "name     STRING, "
                + "url      STRING, "
                + "picture  STRING) "
                + "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'"
                + "LOCATION '/tmp/hive/warehouse/source/' ";

        // load the data - use the URI just to be sure
        String uri = getClass().getClassLoader().getResource("artists.dat").getPath();
        String load = "LOAD DATA INPATH '" + uri + "' OVERWRITE INTO TABLE source";

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

        System.out.println(server.execute(ddl));
        System.out.println(server.execute(localTable));
        System.out.println(server.execute(load));
        System.out.println(server.execute(selectTest));
        System.out.println(server.execute(insert));
    }
}
