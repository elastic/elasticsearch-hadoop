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

import java.util.Collection;
import java.util.List;

import org.elasticsearch.hadoop.integration.QueryTestParams;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static org.junit.Assert.*;

import static org.elasticsearch.hadoop.integration.hive.HiveSuite.*;

@RunWith(Parameterized.class)
public class HiveSearchTest {

    private static int testInstance = 0;

    @Parameters
    public static Collection<Object[]> queries() {
        return QueryTestParams.params();
    }

    private String query;

    public HiveSearchTest(String query) {
        this.query = query;
    }


    @Before
    public void before() throws Exception {
        provisionEsLib();
    }

    @After
    public void after() throws Exception {
        testInstance++;
        HiveSuite.after();
    }


    @Test
    public void basicLoad() throws Exception {

        String create = "CREATE EXTERNAL TABLE artistsload" + testInstance + "("
                + "id 		BIGINT, "
                + "name 	STRING, "
                + "links 	STRUCT<url:STRING, picture:STRING>) "
                + tableProps("hive/artists");

        String select = "SELECT * FROM artistsload" + testInstance;

        System.out.println(server.execute(create));
        List<String> result = server.execute(select);
        System.out.println(result);
        assertTrue("Hive returned null", containsNoNull(result));
    }

    @Test
    public void basicCountOperator() throws Exception {
        String create = "CREATE EXTERNAL TABLE artistscount" + testInstance + " ("
                + "id       BIGINT, "
                + "name     STRING, "
                + "links    STRUCT<url:STRING, picture:STRING>) "
                + tableProps("hive/artists");

        String select = "SELECT count(*) FROM artistscount" + testInstance;

        System.out.println(server.execute(create));
        List<String> result = server.execute(select);
        System.out.println(result);
        assertTrue("Hive returned null", containsNoNull(result));
    }

    @Test
    @Ignore
    public void basicArrayMapping() throws Exception {
        String create = "CREATE EXTERNAL TABLE compoundarray" + testInstance + " ("
                + "rid      INT, "
                + "mapids   ARRAY<INT>, "
                + "rdata    MAP<STRING, STRING>) "
                + tableProps("hive/compound");

        String select = "SELECT * FROM compoundarray" + testInstance;

        System.out.println(server.execute(create));
        List<String> result = server.execute(select);
        System.out.println(result);
        assertTrue("Hive returned null", containsNoNull(result));
    }

    @Test
    public void basicTimestampLoad() throws Exception {
        String create = "CREATE EXTERNAL TABLE timestampload" + testInstance + " ("
                + "id       BIGINT, "
                + "date     TIMESTAMP, "
                + "name     STRING, "
                + "links    STRUCT<url:STRING, picture:STRING>) "
                + tableProps("hive/artiststimestamp");

        String select = "SELECT date FROM timestampload" + testInstance;
        String select2 = "SELECT unix_timestamp(), date FROM timestampload" + testInstance;

        System.out.println(server.execute(create));
        List<String> result = server.execute(select);
        System.out.println(result);
        assertTrue("Hive returned null", containsNoNull(result));
        System.out.println(server.execute(select2));
    }

    @Test
    @Ignore // cast isn't fully supported for date as it throws CCE
    public void basicDateLoad() throws Exception {
        String create = "CREATE EXTERNAL TABLE dateload" + testInstance + " ("
                + "id       BIGINT, "
                + "date     DATE, "
                + "name     STRING, "
                + "links    STRUCT<url:STRING, picture:STRING>) "
                + tableProps("hive/datesave");

        String select = "SELECT date FROM dateload" + testInstance;
        String select2 = "SELECT unix_timestamp(), date FROM dateload" + testInstance;

        System.out.println(server.execute(create));
        List<String> result = server.execute(select);
        System.out.println(result);
        assertTrue("Hive returned null", containsNoNull(result));
        System.out.println(server.execute(select2));
    }

    @Test
    public void javaMethodInvocation() throws Exception {
        String create = "CREATE EXTERNAL TABLE methodInvocation" + testInstance + " ("
                + "id       BIGINT, "
                + "name     STRING, "
                + "links    STRUCT<url:STRING, picture:STRING>) "
                + tableProps("hive/artists");

        String select = "SELECT java_method(\"java.lang.System\", \"currentTimeMillis\") FROM methodInvocation"  + testInstance + " LIMIT 1";

        System.out.println(server.execute(create));
        List<String> result = server.execute(select);
        assertTrue("Hive returned null", containsNoNull(result));
        assertTrue(containsNoNull(result));
    }

    @Test
    public void columnAliases() throws Exception {
        String create = "CREATE EXTERNAL TABLE aliasload" + testInstance + " ("
                + "daTE     TIMESTAMP, "
                + "Name     STRING, "
                + "links    STRUCT<uRl:STRING, pICture:STRING>) "
                + tableProps("hive/aliassave", "'es.mapping.names' = 'daTE:@timestamp, uRl:url_123'");

        String select = "SELECT * FROM aliasload" + testInstance;

        System.out.println(server.execute(create));
        List<String> result = server.execute(select);
        assertTrue("Hive returned null", containsNoNull(result));
        assertTrue(containsNoNull(result));
    }

    @Test
    public void testMissingIndex() throws Exception {
        String create = "CREATE EXTERNAL TABLE missing" + testInstance + " ("
                + "daTE     TIMESTAMP, "
                + "Name     STRING, "
                + "links    STRUCT<uRl:STRING, pICture:STRING>) "
                + tableProps("foobar/missing", "'es.index.read.missing.as.empty' = 'true'");

        String select = "SELECT * FROM missing" + testInstance;

        System.out.println(server.execute(create));
        List<String> result = server.execute(select);
        assertTrue("Hive returned null", containsNoNull(result));
        assertTrue(containsNoNull(result));
    }

    @Test
    public void testVarcharLoad() throws Exception {

        String create = "CREATE EXTERNAL TABLE varcharload" + testInstance + " ("
                + "id       BIGINT, "
                + "name     STRING, "
                + "links    STRUCT<url:STRING, picture:STRING>) "
                + tableProps("hive/varcharsave");

        String select = "SELECT * FROM varcharload" + testInstance;

        System.out.println(server.execute(create));
        List<String> result = server.execute(select);
        System.out.println(result);
        assertTrue("Hive returned null", containsNoNull(result));
    }

    @Test
    public void testParentChild() throws Exception {
        String create = "CREATE EXTERNAL TABLE childload" + testInstance + " ("
                + "id       BIGINT, "
                + "name     STRING, "
                + "links    STRUCT<url:STRING, picture:STRING>) "
                + tableProps("hive/child", "'es.index.read.missing.as.empty' = 'true'");

        String select = "SELECT * FROM childload" + testInstance;

        System.out.println(server.execute(create));
        List<String> result = server.execute(select);
        assertTrue("Hive returned null", containsNoNull(result));
        assertTrue(containsNoNull(result));
    }

    private static boolean containsNoNull(List<String> str) {
        for (String string : str) {
            if (string.contains("NULL")) {
                return false;
            }
        }

        return true;
    }

    private String tableProps(String resource, String... params) {
        return HiveSuite.tableProps(resource, query, params);
    }
}