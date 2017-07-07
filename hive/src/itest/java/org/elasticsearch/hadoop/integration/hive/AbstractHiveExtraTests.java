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

import java.util.List;

import org.elasticsearch.hadoop.mr.RestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import static org.elasticsearch.hadoop.integration.hive.HiveSuite.provisionEsLib;
import static org.elasticsearch.hadoop.integration.hive.HiveSuite.server;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class AbstractHiveExtraTests {

    @Before
    public void before() throws Exception {
        provisionEsLib();
        HiveSuite.before();
    }

    @After
    public void after() throws Exception {
        HiveSuite.after();
    }

    @Test
    public void testQuery() throws Exception {
        if (!RestUtils.exists("cars/transactions")) {
            RestUtils.bulkData("cars/transactions", "cars-bulk.txt");
            RestUtils.refresh("cars");
        }


        String drop = "DROP TABLE IF EXISTS cars2";
        String create = "CREATE EXTERNAL TABLE cars2 ("
                + "color STRING,"
                + "price BIGINT,"
                + "sold TIMESTAMP, "
                + "alias STRING) "
                + HiveSuite.tableProps("cars/transactions", null, "'es.mapping.names'='alias:&c'");

        String query = "SELECT * from cars2";
        String count = "SELECT count(1) from cars2";

        server.execute(drop);
        server.execute(create);
        List<String> result = server.execute(query);
        assertEquals(6, result.size());
        assertTrue(result.get(0).contains("foobar"));
        result = server.execute(count);
        assertEquals("6", result.get(0));
    }

    @Test
    public void testDate() throws Exception {
        String resource = "hive/date-as-long";
        RestUtils.touch("hive");
        RestUtils.putMapping(resource, "org/elasticsearch/hadoop/hive/hive-date-mapping.json");

        RestUtils.postData(resource + "/1", "{\"type\" : 1, \"&t\" : 1407239910771}".getBytes());

        RestUtils.refresh("hive");

        String drop = "DROP TABLE IF EXISTS nixtime";
        String create = "CREATE EXTERNAL TABLE nixtime ("
                + "type     BIGINT,"
                + "dte     TIMESTAMP)"
                + HiveSuite.tableProps("hive/date-as-long", null, "'es.mapping.names'='dte:&t'");

        String query = "SELECT * from nixtime WHERE type = 1";

        String string = RestUtils.get(resource + "/1");
        assertThat(string, containsString("140723"));

        server.execute(drop);
        server.execute(create);
        List<String> result = server.execute(query);

        assertThat(result.size(), is(1));
        assertThat(result.toString(), containsString("2014-08-05"));
    }
}