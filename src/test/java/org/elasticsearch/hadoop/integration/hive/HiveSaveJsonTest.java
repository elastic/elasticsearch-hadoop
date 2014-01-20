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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hive.service.HiveServerException;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.util.RestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import static org.elasticsearch.hadoop.integration.hive.HiveSuite.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class HiveSaveJsonTest {


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
                + "json     STRING) "
                + tableProps("json-hive/artists");

        // transfer data
        String insert =
                "INSERT OVERWRITE TABLE artistssave "
                + "SELECT s.json FROM source s";

        System.out.println(ddl);
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
                + "data     STRING) "
                + tableProps("json-hive/externalserde");

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
                + "json     VARCHAR(255))"
                + tableProps("json-hive/varcharsave");

        // transfer data
        String insert =
                "INSERT OVERWRITE TABLE varcharsave "
                + "SELECT s.json FROM varcharsource s";

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
                + "json     STRING) "
                + tableProps("json-hive/createsave",
                             "'" + ConfigurationOptions.ES_MAPPING_ID + "'='number'",
                             "'" + ConfigurationOptions.ES_WRITE_OPERATION + "'='create'");

        // transfer data
        String insert =
                "INSERT OVERWRITE TABLE createsave "
                + "SELECT s.json FROM createsource s";

        System.out.println(ddl);
        System.out.println(server.execute(ddl));
        System.out.println(server.execute(localTable));
        System.out.println(server.execute(load));
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
                + "json     STRING) "
                + tableProps("json-hive/createsave",
                             "'" + ConfigurationOptions.ES_MAPPING_ID + "'='number'",
                             "'" + ConfigurationOptions.ES_WRITE_OPERATION + "'='create'");

        String selectTest = "SELECT s.json FROM createsourceduplicate s";

        // transfer data
        String insert =
                "INSERT OVERWRITE TABLE createsaveduplicate "
                + "SELECT s.json FROM createsourceduplicate s";

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
                + "json     STRING) "
                + tableProps("json-hive/updatesave",
                             "'" + ConfigurationOptions.ES_MAPPING_ID + "'='number'",
                             "'" + ConfigurationOptions.ES_WRITE_OPERATION + "'='update'");

        String selectTest = "SELECT s.json FROM updatesource s";

        // transfer data
        String insert =
                "INSERT OVERWRITE TABLE updatesave "
                + "SELECT s.json FROM updatesource s";

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
                + "json     STRING) "
                + tableProps("json-hive/updatewoupsertsave",
                             "'" + ConfigurationOptions.ES_MAPPING_ID + "'='number'",
                             "'" + ConfigurationOptions.ES_WRITE_OPERATION + "'='update'",
                             "'" + ConfigurationOptions.ES_UPSERT_DOC + "'='false'");

        String selectTest = "SELECT s.json FROM updatewoupsertsource s";

        // transfer data
        String insert =
                "INSERT OVERWRITE TABLE updatewoupsertsave "
                + "SELECT s.json FROM updatewoupsertsource s";

        System.out.println(ddl);
        System.out.println(server.execute(ddl));
        System.out.println(server.execute(localTable));
        System.out.println(server.execute(load));
        System.out.println(server.execute(selectTest));
        System.out.println(server.execute(insert));
    }

    @Test
    public void testParentChild() throws Exception {
        RestUtils.putMapping("json-hive/child", "org/elasticsearch/hadoop/integration/mr-child.json");

        String localTable = createTable("childsource");
        String load = loadData("childsource");

        // create external table
        String ddl =
                "CREATE EXTERNAL TABLE child ("
                + "json     STRING) "
                + tableProps("json-hive/child",
                             "'" + ConfigurationOptions.ES_MAPPING_PARENT + "'='number'",
                             "'" + ConfigurationOptions.ES_INDEX_AUTO_CREATE + "'='false'");

        String selectTest = "SELECT s.json FROM childsource s";

        // transfer data
        String insert =
                "INSERT OVERWRITE TABLE child "
                + "SELECT s.json FROM childsource s";

        System.out.println(ddl);
        System.out.println(server.execute(ddl));
        System.out.println(server.execute(localTable));
        System.out.println(server.execute(load));
        System.out.println(server.execute(selectTest));
        System.out.println(server.execute(insert));
    }


    private String createTable(String tableName) {
        return String.format("CREATE TABLE %s ("
                + "json     STRING) "
        //                + "LINES TERMINATED BY '\n'"
                + "LOCATION '/tmp/hive/warehouse/%s/' "
                , tableName, tableName);
    }

    private String loadData(String tableName) {
        return "LOAD DATA " + (isLocal ? "LOCAL" : "") + " INPATH '" + HiveSuite.hdfsJsonResource + "' OVERWRITE INTO TABLE " + tableName;
    }

    private static String tableProps(String resource, String... params) {
        List<String> parms = new ArrayList<String>();
        parms.add("'es.input.json'='true'");
        if (params != null) {
            Collections.addAll(parms, params);
        }
        return HiveSuite.tableProps(resource, null, parms.toArray(new String[parms.size()]));
    }
}