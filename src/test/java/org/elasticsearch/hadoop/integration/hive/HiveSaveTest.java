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

        //String jar = "ADD JAR /tmp/es-hadoop.jar";
        String localTable = "CREATE TABLE source ("
                + "id       BIGINT, "
                + "name     STRING, "
                + "url      STRING, "
                + "picture  STRING) "
                + "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'"
                + "LOCATION '/tmp/hive/warehouse/source/' ";

        String load = loadData("source");

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

        //System.out.println(server.execute(jar));
        System.out.println(server.execute(ddl));
        System.out.println(server.execute(localTable));
        System.out.println(server.execute(load));
        System.out.println(server.execute(selectTest));
        System.out.println(server.execute(insert));
    }

    @Test
    // see http://shmsoft.blogspot.ro/2011/10/loading-inner-maps-in-hive.html
    public void testCompoundSave() throws Exception {

        //String jar = "ADD JAR /tmp/es-hadoop.jar";

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
        String load = loadData("compoundSource");


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

        //System.out.println(server.execute(jar));
        System.out.println(server.execute(localTable));
        System.out.println(server.execute(load));
        System.out.println(server.execute(selectTest));
        System.out.println(server.execute(ddl));
        System.out.println(server.execute(insert));
    }

    @Test
    public void testTimestampSave() throws Exception {
        String localTable = "CREATE TABLE timestampsource ("
                + "id       BIGINT, "
                + "name     STRING, "
                + "url      STRING, "
                + "picture  STRING) "
                + "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'"
                + "LOCATION '/tmp/hive/warehouse/timestampsource/' ";

        String load = loadData("timestampsource");

        // create external table
        String ddl =
                "CREATE EXTERNAL TABLE artiststimestampsave ("
                + "id       BIGINT, "
                + "date     TIMESTAMP, "
                + "name     STRING, "
                + "links    STRUCT<url:STRING, picture:STRING>) "
                + "STORED BY 'org.elasticsearch.hadoop.hive.ESStorageHandler' "
                + "WITH SERDEPROPERTIES ('serder.foo' = 'serder.bar') "
                + "TBLPROPERTIES('es.resource' = 'hive/artiststimestamp') ";

        String currentDate = "SELECT *, from_unixtime(unix_timestamp()) from timestampsource";

        // since the date format is different in Hive vs ISO8601/Joda, save only the date (which is the same) as a string
        // we do this since unix_timestamp() saves the date as a long (in seconds) and w/o mapping the date is not recognized as data
        String insert =
                "INSERT OVERWRITE TABLE artiststimestampsave "
                + "SELECT NULL, from_unixtime(unix_timestamp()), s.name, named_struct('url', s.url, 'picture', s.picture) FROM timestampsource s";

        //System.out.println(server.execute(jar));
        System.out.println(server.execute(ddl));
        System.out.println(server.execute(localTable));
        System.out.println(server.execute(load));
        System.out.println(server.execute(currentDate));
        System.out.println(server.execute(insert));
    }

    @Test
    public void testFieldAlias() throws Exception {
        String localTable = "CREATE TABLE aliassource ("
                + "id       BIGINT, "
                + "name     STRING, "
                + "url      STRING, "
                + "picture  STRING) "
                + "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'"
                + "LOCATION '/tmp/hive/warehouse/aliassource/' ";

        String load = loadData("aliassource");

        // create external table
        String ddl =
                "CREATE EXTERNAL TABLE aliassave ("
                + "daTE     TIMESTAMP, "
                + "Name     STRING, "
                + "links    STRUCT<uRl:STRING, pICture:STRING>) "
                + "STORED BY 'org.elasticsearch.hadoop.hive.ESStorageHandler' "
                + "WITH SERDEPROPERTIES ('serder.foo' = 'serder.bar') "
                + "TBLPROPERTIES('es.resource' = 'hive/aliassave' ," +
                                "'es.column.aliases' = 'daTE:@timestamp, uRl:url_123')";

        // since the date format is different in Hive vs ISO8601/Joda, save only the date (which is the same) as a string
        // we do this since unix_timestamp() saves the date as a long (in seconds) and w/o mapping the date is not recognized as data
        String insert =
                "INSERT OVERWRITE TABLE aliassave "
                + "SELECT from_unixtime(unix_timestamp()), s.name, named_struct('uRl', s.url, 'pICture', s.picture) FROM aliassource s";

        //System.out.println(server.execute(jar));
        System.out.println(server.execute(ddl));
        System.out.println(server.execute(localTable));
        System.out.println(server.execute(load));
        System.out.println(server.execute(insert));
    }

    @Test
    public void testExternalSerDe() throws Exception {
        String localTable = "CREATE TABLE externalserde ("
                + "data       STRING) "
                + "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe' "
                + "WITH SERDEPROPERTIES ('input.regex'='(.*)') "
                + "LOCATION '/tmp/hive/warehouse/externalserde/' ";

        String load = loadData("externalserde");

        // create external table
        String ddl =
                "CREATE EXTERNAL TABLE externalserdetest ("
                + "data     STRING)"
                + "STORED BY 'org.elasticsearch.hadoop.hive.ESStorageHandler' "
                + "WITH SERDEPROPERTIES ('serder.foo' = 'serder.bar') "
                + "TBLPROPERTIES('es.resource' = 'hive/externalserde')";

        String insert =
                "INSERT OVERWRITE TABLE externalserdetest "
                + "SELECT s.data FROM externalserde s";

        //System.out.println(server.execute(jar));
        System.out.println(server.execute(ddl));
        System.out.println(server.execute(localTable));
        System.out.println(server.execute(load));
        System.out.println(server.execute(insert));
    }

    private String loadData(String tableName) {
        return "LOAD DATA " + (isLocal ? "LOCAL" : "") + " INPATH '" + HiveSuite.hdfsResource + "' OVERWRITE INTO TABLE " + tableName;
    }
}