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

import java.util.List;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.*;

import static org.elasticsearch.hadoop.integration.hive.HiveSuite.*;

public class HiveSearchTest {

    @Before
    public void provision() throws Exception {
        provisionEsLib();
    }


    @Test
    public void basicLoad() throws Exception {

        String create = "CREATE EXTERNAL TABLE artistsload ("
                + "id 		BIGINT, "
                + "name 	STRING, "
                + "links 	STRUCT<url:STRING, picture:STRING>) "
                + tableProps("hive/artists/_search?q=*");

        String select = "SELECT * FROM artistsload";

        System.out.println(server.execute(create));
        List<String> result = server.execute(select);
        System.out.println(result);
        assertTrue("Hive returned null", containsNoNull(result));
    }

    @Test
    public void basicCountOperator() throws Exception {
        String create = "CREATE EXTERNAL TABLE artistscount ("
                + "id       BIGINT, "
                + "name     STRING, "
                + "links    STRUCT<url:STRING, picture:STRING>) "
                + tableProps("hive/artists/_search?q=*");

        String select = "SELECT count(*) FROM artistscount";

        System.out.println(server.execute(create));
        List<String> result = server.execute(select);
        System.out.println(result);
        assertTrue("Hive returned null", containsNoNull(result));
    }

    @Test
    @Ignore
    public void basicArrayMapping() throws Exception {
        String create = "CREATE EXTERNAL TABLE compoundarray ("
                + "rid      INT, "
                + "mapids   ARRAY<INT>, "
                + "rdata    MAP<STRING, STRING>) "
                + tableProps("hive/compound/_search?q=*");

        String select = "SELECT * FROM compoundarray";

        System.out.println(server.execute(create));
        List<String> result = server.execute(select);
        System.out.println(result);
        assertTrue("Hive returned null", containsNoNull(result));
    }

    @Test
    public void basicTimestampLoad() throws Exception {
        String create = "CREATE EXTERNAL TABLE timestampload ("
                + "id       BIGINT, "
                + "date     TIMESTAMP, "
                + "name     STRING, "
                + "links    STRUCT<url:STRING, picture:STRING>) "
                + tableProps("hive/artiststimestamp/_search?q=*");

        String select = "SELECT date FROM timestampload";
        String select2 = "SELECT unix_timestamp(), date FROM timestampload";

        System.out.println(server.execute(create));
        List<String> result = server.execute(select);
        System.out.println(result);
        assertTrue("Hive returned null", containsNoNull(result));
        System.out.println(server.execute(select2));
    }

    @Test
    public void javaMethodInvocation() throws Exception {
        String create = "CREATE EXTERNAL TABLE methodInvocation ("
                + "id       BIGINT, "
                + "name     STRING, "
                + "links    STRUCT<url:STRING, picture:STRING>) "
                + tableProps("hive/artists/_search?q=*");

        String select = "SELECT java_method(\"java.lang.System\", \"currentTimeMillis\") FROM methodInvocation LIMIT 1";

        System.out.println(server.execute(create));
        List<String> result = server.execute(select);
        assertTrue("Hive returned null", containsNoNull(result));
        assertTrue(containsNoNull(result));
    }

    @Test
    public void columnAliases() throws Exception {
        String create = "CREATE EXTERNAL TABLE aliasload ("
                + "daTE     TIMESTAMP, "
                + "Name     STRING, "
                + "links    STRUCT<uRl:STRING, pICture:STRING>) "
                + tableProps("hive/aliassave/_search?q=*", "'es.mapping.names' = 'daTE:@timestamp, uRl:url_123'");

        String select = "SELECT * FROM aliasload";

        System.out.println(server.execute(create));
        List<String> result = server.execute(select);
        assertTrue("Hive returned null", containsNoNull(result));
        assertTrue(containsNoNull(result));
    }

    @Test
    public void testMissingIndex() throws Exception {
        String create = "CREATE EXTERNAL TABLE missing ("
                + "daTE     TIMESTAMP, "
                + "Name     STRING, "
                + "links    STRUCT<uRl:STRING, pICture:STRING>) "
                + tableProps("foobar/missing/_search?q=*", "'es.index.read.missing.as.empty' = 'true'");

        String select = "SELECT * FROM missing";

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
}