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
package org.elasticsearch.hadoop.integration.hive;

import org.apache.hadoop.hive.service.HiveServerException;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.mr.RestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import static org.elasticsearch.hadoop.integration.hive.HiveSuite.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class AbstractHiveSaveTest {


    @Before
    public void before() throws Exception {
        HiveSuite.before();
    }

    @After
    public void after() throws Exception {
        HiveSuite.after();
    }

    @Test
    public void testBasicSave() throws Exception {
        // load the raw data as a native, managed table
        // and then insert its content into the external one

        String localTable = createTable("source");
        String load = loadData("source");

        // create external table
        String ddl =
                "CREATE EXTERNAL TABLE artistssave ("
                + "id       BIGINT, "
                + "name     STRING, "
                + "links    STRUCT<url:STRING, picture:STRING>) "
                + tableProps("hive/artists");

        String selectTest = "SELECT s.name, struct(s.url, s.picture) FROM source s";

        // transfer data
        String insert =
                "INSERT OVERWRITE TABLE artistssave "
                + "SELECT s.id, s.name, named_struct('url', s.url, 'picture', s.picture) FROM source s";

        System.out.println(ddl);
        System.out.println(server.execute(ddl));
        System.out.println(server.execute(localTable));
        System.out.println(server.execute(load));
        System.out.println(server.execute(selectTest));
        System.out.println(server.execute(insert));
    }

    @Test
    // see http://shmsoft.blogspot.ro/2011/10/loading-inner-maps-in-hive.html
    public void testCompoundSave() throws Exception {

        // load the raw data as a native, managed table
        // and then insert its content into the external one
        String localTable = "CREATE TABLE compoundsource ("
                + "rid      INT, "
                + "name     STRING, "
                + "url      STRING, "
                + "picture  STRING, "
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
                + tableProps("hive/compound");

        String selectTest = "SELECT rid, mapids, rdata FROM compoundsource";

        // transfer data
        String insert =
                "INSERT OVERWRITE TABLE compoundsave "
                + "SELECT rid, mapids, rdata FROM compoundsource";

        System.out.println(ddl);
        System.out.println(server.execute(localTable));
        System.out.println(server.execute(load));
        System.out.println(server.execute(selectTest));
        System.out.println(server.execute(ddl));
        System.out.println(server.execute(insert));
    }

    @Test
    public void testTimestampSave() throws Exception {
        String localTable = createTable("timestampsource");
        String load = loadData("timestampsource");

        // create external table
        String ddl =
                "CREATE EXTERNAL TABLE artiststimestampsave ("
                + "id       BIGINT, "
                + "date     TIMESTAMP, "
                + "name     STRING, "
                + "links    STRUCT<url:STRING, picture:STRING>) "
                + tableProps("hive/artiststimestamp");

        String currentDate = "SELECT *, from_unixtime(unix_timestamp()) from timestampsource";

        // since the date format is different in Hive vs ISO8601/Joda, save only the date (which is the same) as a string
        // we do this since unix_timestamp() saves the date as a long (in seconds) and w/o mapping the date is not recognized as data
        String insert =
                "INSERT OVERWRITE TABLE artiststimestampsave "
                + "SELECT NULL, from_unixtime(unix_timestamp()), s.name, named_struct('url', s.url, 'picture', s.picture) FROM timestampsource s";

        System.out.println(ddl);
        System.out.println(server.execute(ddl));
        System.out.println(server.execute(localTable));
        System.out.println(server.execute(load));
        System.out.println(server.execute(currentDate));
        System.out.println(server.execute(insert));
    }

    @Test
    public void testFieldAlias() throws Exception {
        String localTable = createTable("aliassource");
        String load = loadData("aliassource");

        // create external table
        String ddl =
                "CREATE EXTERNAL TABLE aliassave ("
                + "daTE     TIMESTAMP, "
                + "Name     STRING, "
                + "links    STRUCT<uRl:STRING, pICture:STRING>) "
                + tableProps("hive/aliassave", "'es.mapping.names' = 'daTE:@timestamp, uRl:url_123'");

        // since the date format is different in Hive vs ISO8601/Joda, save only the date (which is the same) as a string
        // we do this since unix_timestamp() saves the date as a long (in seconds) and w/o mapping the date is not recognized as data
        String insert =
                "INSERT OVERWRITE TABLE aliassave "
                + "SELECT from_unixtime(unix_timestamp()), s.name, named_struct('uRl', s.url, 'pICture', s.picture) FROM aliassource s";

        System.out.println(ddl);
        System.out.println(server.execute(ddl));
        System.out.println(server.execute(localTable));
        System.out.println(server.execute(load));
        System.out.println(server.execute(insert));
    }

    @Test
    @Ignore // cast isn't fully supported for date as it throws CCE
    public void testDateSave() throws Exception {
        String localTable = createTable("datesource");
        String load = loadData("datesource");

        // create external table
        String ddl =
                "CREATE EXTERNAL TABLE datesave ("
                + "id       BIGINT, "
                + "date     DATE, "
                + "name     STRING, "
                + "links    STRUCT<url:STRING, picture:STRING>) "
                + tableProps("hive/datesave");

        // this works
        String someDate = "SELECT cast('2013-10-21' as date) from datesource";


        // this does not
        String insert =
                "INSERT OVERWRITE TABLE datesave "
                + "SELECT NULL, cast('2013-10-21' as date), s.name, named_struct('url', s.url, 'picture', s.picture) FROM datesource s";

        System.out.println(ddl);
        System.out.println(server.execute(ddl));
        System.out.println(server.execute(localTable));
        System.out.println(server.execute(load));
        System.out.println(server.execute(someDate));
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
                + tableProps("hive/externalserde");

        String insert =
                "INSERT OVERWRITE TABLE externalserdetest "
                + "SELECT s.data FROM externalserde s";

        System.out.println(ddl);
        System.out.println(server.execute(ddl));
        System.out.println(server.execute(localTable));
        System.out.println(server.execute(load));
        System.out.println(server.execute(insert));
    }

    @Test
    public void testVarcharSave() throws Exception {
        String localTable = createTable("varcharsource");
        String load = loadData("varcharsource");

        // create external table
        String ddl =
                "CREATE EXTERNAL TABLE varcharsave ("
                + "id       BIGINT, "
                + "name     VARCHAR(10), "
                + "links    STRUCT<url:STRING, picture:STRING>) "
                + tableProps("hive/varcharsave");

        // transfer data
        String insert =
                "INSERT OVERWRITE TABLE varcharsave "
                + "SELECT s.id, s.name, named_struct('url', s.url, 'picture', s.picture) FROM varcharsource s";

        System.out.println(ddl);
        System.out.println(server.execute(ddl));
        System.out.println(server.execute(localTable));
        System.out.println(server.execute(load));
        System.out.println(server.execute(insert));
    }

    @Test
    public void testCreate() throws Exception {
        // load the raw data as a native, managed table
        // and then insert its content into the external one

        String localTable = createTable("createsource");
        String load = loadData("createsource");

        // create external table
        String ddl =
                "CREATE EXTERNAL TABLE createsave ("
                + "id       BIGINT, "
                + "name     STRING, "
                + "links    STRUCT<url:STRING, picture:STRING>) "
                + tableProps("hive/createsave",
                             "'" + ConfigurationOptions.ES_MAPPING_ID + "'='id'",
                             "'" + ConfigurationOptions.ES_WRITE_OPERATION + "'='create'");

        String selectTest = "SELECT s.name, struct(s.url, s.picture) FROM createsource s";

        // transfer data
        String insert =
                "INSERT OVERWRITE TABLE createsave "
                + "SELECT s.id, s.name, named_struct('url', s.url, 'picture', s.picture) FROM createsource s";

        System.out.println(ddl);
        System.out.println(server.execute(ddl));
        System.out.println(server.execute(localTable));
        System.out.println(server.execute(load));
        System.out.println(server.execute(selectTest));
        System.out.println(server.execute(insert));
    }

    @Test(expected = HiveServerException.class)
    public void testCreateWithDuplicates() throws Exception {
        // load the raw data as a native, managed table
        // and then insert its content into the external one

        String localTable = createTable("createsourceduplicate");
        String load = loadData("createsourceduplicate");

        // create external table
        String ddl =
                "CREATE EXTERNAL TABLE createsaveduplicate ("
                + "id       BIGINT, "
                + "name     STRING, "
                + "links    STRUCT<url:STRING, picture:STRING>) "
                + tableProps("hive/createsave",
                             "'" + ConfigurationOptions.ES_MAPPING_ID + "'='id'",
                             "'" + ConfigurationOptions.ES_WRITE_OPERATION + "'='create'");

        String selectTest = "SELECT s.name, struct(s.url, s.picture) FROM createsourceduplicate s";

        // transfer data
        String insert =
                "INSERT OVERWRITE TABLE createsaveduplicate "
                + "SELECT s.id, s.name, named_struct('url', s.url, 'picture', s.picture) FROM createsourceduplicate s";

        System.out.println(ddl);
        System.out.println(server.execute(ddl));
        System.out.println(server.execute(localTable));
        System.out.println(server.execute(load));
        System.out.println(server.execute(selectTest));
        System.out.println(server.execute(insert));
    }

    @Test
    public void testUpdateWithId() throws Exception {
        // load the raw data as a native, managed table
        // and then insert its content into the external one

        String localTable = createTable("updatesource");
        String load = loadData("updatesource");

        // create external table
        String ddl =
                "CREATE EXTERNAL TABLE updatesave ("
                + "id       BIGINT, "
                + "name     STRING, "
                + "links    STRUCT<url:STRING, picture:STRING>) "
                + tableProps("hive/updatesave",
                             "'" + ConfigurationOptions.ES_MAPPING_ID + "'='id'",
                             "'" + ConfigurationOptions.ES_WRITE_OPERATION + "'='update'");

        String selectTest = "SELECT s.name, struct(s.url, s.picture) FROM updatesource s";

        // transfer data
        String insert =
                "INSERT OVERWRITE TABLE updatesave "
                + "SELECT s.id, s.name, named_struct('url', s.url, 'picture', s.picture) FROM updatesource s";

        System.out.println(ddl);
        System.out.println(server.execute(ddl));
        System.out.println(server.execute(localTable));
        System.out.println(server.execute(load));
        System.out.println(server.execute(selectTest));
        System.out.println(server.execute(insert));
    }


    @Test(expected = HiveServerException.class)
    public void testUpdateWithoutUpsert() throws Exception {
        // load the raw data as a native, managed table
        // and then insert its content into the external one

        String localTable = createTable("updatewoupsertsource");
        String load = loadData("updatewoupsertsource");

        // create external table
        String ddl =
                "CREATE EXTERNAL TABLE updatewoupsertsave ("
                + "id       BIGINT, "
                + "name     STRING, "
                + "links    STRUCT<url:STRING, picture:STRING>) "
                + tableProps("hive/updatewoupsertsave",
                             "'" + ConfigurationOptions.ES_MAPPING_ID + "'='id'",
                             "'" + ConfigurationOptions.ES_WRITE_OPERATION + "'='update'",
                             "'" + ConfigurationOptions.ES_UPSERT_DOC + "'='false'");

        String selectTest = "SELECT s.name, struct(s.url, s.picture) FROM updatewoupsertsource s";

        // transfer data
        String insert =
                "INSERT OVERWRITE TABLE updatewoupsertsave "
                + "SELECT s.id, s.name, named_struct('url', s.url, 'picture', s.picture) FROM updatewoupsertsource s";

        System.out.println(ddl);
        System.out.println(server.execute(ddl));
        System.out.println(server.execute(localTable));
        System.out.println(server.execute(load));
        System.out.println(server.execute(selectTest));
        System.out.println(server.execute(insert));
    }

    @Test
    public void testParentChild() throws Exception {
        RestUtils.putMapping("hive/child", "org/elasticsearch/hadoop/integration/mr-child.json");

        String localTable = createTable("childsource");
        String load = loadData("childsource");

        // create external table
        String ddl =
                "CREATE EXTERNAL TABLE child ("
                + "id       BIGINT, "
                + "name     STRING, "
                + "links    STRUCT<url:STRING, picture:STRING>) "
                + tableProps("hive/child",
                             "'" + ConfigurationOptions.ES_MAPPING_PARENT + "'='id'",
                             "'" + ConfigurationOptions.ES_INDEX_AUTO_CREATE + "'='false'");

        String selectTest = "SELECT s.id, struct(s.url, s.picture) FROM childsource s";

        // transfer data
        String insert =
                "INSERT OVERWRITE TABLE child "
                + "SELECT s.id, s.name, named_struct('url', s.url, 'picture', s.picture) FROM childsource s";

        System.out.println(ddl);
        System.out.println(server.execute(ddl));
        System.out.println(server.execute(localTable));
        System.out.println(server.execute(load));
        System.out.println(server.execute(selectTest));
        System.out.println(server.execute(insert));
    }

    @Test
    public void testIndexPattern() throws Exception {
        // load the raw data as a native, managed table
        // and then insert its content into the external one

        String localTable = createTable("sourcepattern");
        String load = loadData("sourcepattern");

        // create external table
        String ddl =
                "CREATE EXTERNAL TABLE pattern ("
                + "id       BIGINT, "
                + "name     STRING, "
                + "links    STRUCT<url:STRING, picture:STRING>) "
                + tableProps("hive/pattern-{id}");

        String selectTest = "SELECT s.name, struct(s.url, s.picture) FROM sourcepattern s";

        // transfer data
        String insert =
                "INSERT OVERWRITE TABLE pattern "
                + "SELECT s.id, s.name, named_struct('url', s.url, 'picture', s.picture) FROM sourcepattern s";

        System.out.println(ddl);
        System.out.println(server.execute(ddl));
        System.out.println(server.execute(localTable));
        System.out.println(server.execute(load));
        System.out.println(server.execute(selectTest));
        System.out.println(server.execute(insert));
    }


    private String createTable(String tableName) {
        return String.format("CREATE TABLE %s ("
                + "id       BIGINT, "
                + "name     STRING, "
                + "url      STRING, "
                + "picture  STRING, "
                + "ignore1  STRING, "
                + "ignore2  STRING) "
                + "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'"
                + "LOCATION '/tmp/hive/warehouse/%s/' "
                , tableName, tableName);
    }

    private String loadData(String tableName) {
        return "LOAD DATA " + (isLocal ? "LOCAL" : "") + " INPATH '" + HiveSuite.hdfsResource + "' OVERWRITE INTO TABLE " + tableName;
    }

    private static String tableProps(String resource, String... params) {
        return HiveSuite.tableProps(resource, null, params);
    }
}