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

import static org.junit.Assert.*;

import static org.elasticsearch.hadoop.integration.hive.HiveSuite.*;

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
        RestUtils.bulkData("cars/transactions", "cars-bulk.txt");
        RestUtils.refresh("cars");

        String create = "CREATE EXTERNAL TABLE cars2 ("
                + "color STRING,"
                + "price BIGINT,"
                + "sold TIMESTAMP) "
                + HiveSuite.tableProps("cars/transactions", null, null);

        String query = "SELECT * from cars2";
        String count = "SELECT count(1) from cars2";

        server.execute(create);
        List<String> result = server.execute(query);
        assertEquals(6, result.size());
        result = server.execute(count);
        assertEquals("6", result.get(0));
    }
}
