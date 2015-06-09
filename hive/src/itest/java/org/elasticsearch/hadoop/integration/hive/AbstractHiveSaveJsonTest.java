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

import org.apache.hive.service.cli.HiveSQLException;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.mr.RestUtils;
import org.elasticsearch.hadoop.util.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import static org.elasticsearch.hadoop.integration.hive.HiveSuite.isLocal;
import static org.elasticsearch.hadoop.integration.hive.HiveSuite.server;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class AbstractHiveSaveJsonTest {


    @Before
    public void before() throws Exception {
        HiveSuite.before();
    }

    @After
    public void after() throws Exception {
        HiveSuite.after();
    }

    @Test
    public void testNestedFields() throws Exception {
        String data = "{ \"data\" : { \"map\" : { \"key\" : [ 10, 20 ] } } }";
        RestUtils.postData("jsonnestedmap", StringUtils.toUTF(data));
    }

    @Test
    public void testBasicSave() throws Exception {
        // load the raw data as a native, managed table
        // and then insert its content into the external one

        String localTable = createTable("jsonsource");
        String load = loadData("jsonsource");

        // create external table
        String ddl =
                "CREATE EXTERNAL TABLE jsonartistssave ("
                        + "json     STRING) "
                        + tableProps("json-hive/artists");

        // transfer data
        String insert =
                "INSERT OVERWRITE TABLE jsonartistssave "
                        + "SELECT s.json FROM jsonsource s";

        System.out.println(ddl);
        System.out.println(server.execute(ddl));
        System.out.println(server.execute(localTable));
        System.out.println(server.execute(load));
        System.out.println(server.execute(insert));
    }

    @Test
    public void testExternalSerDe() throws Exception {
        String localTable = "CREATE TABLE jsonexternalserde ("
                + "data       STRING) "
                + "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe' "
                + "WITH SERDEPROPERTIES ('input.regex'='(.*)') "
                + "LOCATION '/tmp/hive/warehouse/jsonexternalserde/' ";

        String load = loadData("jsonexternalserde");

        // create external table
        String ddl =
                "CREATE EXTERNAL TABLE jsonexternalserdetest ("
                        + "data     STRING) "
                        + tableProps("json-hive/externalserde");

        String insert =
                "INSERT OVERWRITE TABLE jsonexternalserdetest "
                        + "SELECT s.data FROM jsonexternalserde s";

        System.out.println(ddl);
        System.out.println(server.execute(ddl));
        System.out.println(server.execute(localTable));
        System.out.println(server.execute(load));
        System.out.println(server.execute(insert));
    }

    @Test
    public void testVarcharSave() throws Exception {
        String localTable = createTable("jsonvarcharsource");
        String load = loadData("jsonvarcharsource");

        // create external table
        String ddl =
                "CREATE EXTERNAL TABLE jsonvarcharsave ("
                        + "json     VARCHAR(255))"
                        + tableProps("json-hive/varcharsave");

        // transfer data
        String insert =
                "INSERT OVERWRITE TABLE jsonvarcharsave "
                        + "SELECT s.json FROM jsonvarcharsource s";

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

        String localTable = createTable("jsoncreatesource");
        String load = loadData("jsoncreatesource");

        // create external table
        String ddl =
                "CREATE EXTERNAL TABLE jsoncreatesave ("
                        + "json     STRING) "
                        + tableProps("json-hive/createsave",
                                "'" + ConfigurationOptions.ES_MAPPING_ID + "'='number'",
                                "'" + ConfigurationOptions.ES_WRITE_OPERATION + "'='create'");

        // transfer data
        String insert =
                "INSERT OVERWRITE TABLE jsoncreatesave "
                        + "SELECT s.json FROM jsoncreatesource s";

        System.out.println(ddl);
        System.out.println(server.execute(ddl));
        System.out.println(server.execute(localTable));
        System.out.println(server.execute(load));
        System.out.println(server.execute(insert));
    }

    @Test(expected = HiveSQLException.class)
    public void testCreateWithDuplicates() throws Exception {
        // load the raw data as a native, managed table
        // and then insert its content into the external one

        String localTable = createTable("jsoncreatesourceduplicate");
        String load = loadData("jsoncreatesourceduplicate");

        // create external table
        String ddl =
                "CREATE EXTERNAL TABLE jsoncreatesaveduplicate ("
                        + "json     STRING) "
                        + tableProps("json-hive/createsave",
                                "'" + ConfigurationOptions.ES_MAPPING_ID + "'='number'",
                                "'" + ConfigurationOptions.ES_WRITE_OPERATION + "'='create'");

        String selectTest = "SELECT s.json FROM jsoncreatesourceduplicate s";

        // transfer data
        String insert =
                "INSERT OVERWRITE TABLE jsoncreatesaveduplicate "
                        + "SELECT s.json FROM jsoncreatesourceduplicate s";

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

        String localTable = createTable("jsonupdatesource");
        String load = loadData("jsonupdatesource");

        // create external table
        String ddl =
                "CREATE EXTERNAL TABLE jsonupdatesave ("
                        + "json     STRING) "
                        + tableProps("json-hive/updatesave",
                                "'" + ConfigurationOptions.ES_MAPPING_ID + "'='number'",
                                "'" + ConfigurationOptions.ES_WRITE_OPERATION + "'='upsert'");

        String selectTest = "SELECT s.json FROM jsonupdatesource s";

        // transfer data
        String insert =
                "INSERT OVERWRITE TABLE jsonupdatesave "
                        + "SELECT s.json FROM jsonupdatesource s";

        System.out.println(ddl);
        System.out.println(server.execute(ddl));
        System.out.println(server.execute(localTable));
        System.out.println(server.execute(load));
        System.out.println(server.execute(selectTest));
        System.out.println(server.execute(insert));
    }


    @Test(expected = HiveSQLException.class)
    public void testUpdateWithoutUpsert() throws Exception {
        // load the raw data as a native, managed table
        // and then insert its content into the external one

        String localTable = createTable("jsonupdatewoupsertsource");
        String load = loadData("jsonupdatewoupsertsource");

        // create external table
        String ddl =
                "CREATE EXTERNAL TABLE jsonupdatewoupsertsave ("
                        + "json     STRING) "
                        + tableProps("json-hive/updatewoupsertsave",
                                "'" + ConfigurationOptions.ES_MAPPING_ID + "'='number'",
                                "'" + ConfigurationOptions.ES_WRITE_OPERATION + "'='update'");

        String selectTest = "SELECT s.json FROM jsonupdatewoupsertsource s";

        // transfer data
        String insert =
                "INSERT OVERWRITE TABLE jsonupdatewoupsertsave "
                        + "SELECT s.json FROM jsonupdatewoupsertsource s";

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

        String localTable = createTable("jsonchildsource");
        String load = loadData("jsonchildsource");

        // create external table
        String ddl =
                "CREATE EXTERNAL TABLE jsonchild ("
                        + "json     STRING) "
                        + tableProps("json-hive/child",
                                "'" + ConfigurationOptions.ES_MAPPING_PARENT + "'='number'",
                                "'" + ConfigurationOptions.ES_INDEX_AUTO_CREATE + "'='false'");

        String selectTest = "SELECT s.json FROM jsonchildsource s";

        // transfer data
        String insert =
                "INSERT OVERWRITE TABLE jsonchild "
                        + "SELECT s.json FROM jsonchildsource s";

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

        String localTable = createTable("jsonsourcepattern");
        String load = loadData("jsonsourcepattern");

        // create external table
        // create external table
        String ddl =
                "CREATE EXTERNAL TABLE jsonpattern ("
                        + "json     STRING) "
                        + tableProps("json-hive/pattern-{number}");

        String selectTest = "SELECT s.json FROM jsonsourcepattern s";

        // transfer data
        String insert =
                "INSERT OVERWRITE TABLE jsonpattern "
                        + "SELECT s.json FROM jsonsourcepattern s";

        System.out.println(ddl);
        System.out.println(server.execute(ddl));
        System.out.println(server.execute(localTable));
        System.out.println(server.execute(load));
        System.out.println(server.execute(selectTest));
        System.out.println(server.execute(insert));
    }

    @Test
    public void testIndexPatternFormat() throws Exception {
        // load the raw data as a native, managed table
        // and then insert its content into the external one

        String localTable = createTable("jsonsourcepatternformat");
        String load = loadData("jsonsourcepatternformat");

        // create external table
        // create external table
        String ddl =
                "CREATE EXTERNAL TABLE jsonpatternformat ("
                        + "json     STRING) "
                        + tableProps("json-hive/pattern-format-{@timestamp:YYYY-MM-dd}");

        String selectTest = "SELECT s.json FROM jsonsourcepatternformat s";

        // transfer data
        String insert =
                "INSERT OVERWRITE TABLE jsonpatternformat "
                        + "SELECT s.json FROM jsonsourcepatternformat s";

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