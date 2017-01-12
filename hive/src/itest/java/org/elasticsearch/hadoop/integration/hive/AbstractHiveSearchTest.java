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

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.elasticsearch.hadoop.QueryTestParams;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.mr.RestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static org.junit.Assert.*;

import static org.elasticsearch.hadoop.integration.hive.HiveSuite.*;

@RunWith(Parameterized.class)
public class AbstractHiveSearchTest {

    private static int testInstance = 0;

    @Parameters
    public static Collection<Object[]> queries() {
        return QueryTestParams.params();
    }

    private String query;
    private boolean readMetadata;

    public AbstractHiveSearchTest(String query, boolean readMetadata) {
        this.query = query;
        this.readMetadata = readMetadata;
    }


    @Before
    public void before() throws Exception {
        provisionEsLib();
        RestUtils.refresh("hive");
    }

    @After
    public void after() throws Exception {
        testInstance++;
        HiveSuite.after();
    }


    @Test
    public void basicLoad() throws Exception {

        String create = "CREATE EXTERNAL TABLE artistsload" + testInstance + "("
                + "id         BIGINT, "
                + "name     STRING, "
                + "links     STRUCT<url:STRING, picture:STRING>) "
                + tableProps("hive/artists");

        String select = "SELECT * FROM artistsload" + testInstance;

        server.execute(create);
        List<String> result = server.execute(select);
        assertTrue("Hive returned null", containsNoNull(result));
        assertContains(result, "Marilyn");
        assertContains(result, "last.fm/music/MALICE");
        assertContains(result, "last.fm/serve/252/5872875.jpg");
    }

    //@Test
    public void basicCountOperator() throws Exception {
        String create = "CREATE EXTERNAL TABLE artistscount" + testInstance + " ("
                + "id       BIGINT, "
                + "name     STRING, "
                + "links    STRUCT<url:STRING, picture:STRING>) "
                + tableProps("hive/artists");

        String select = "SELECT count(*) FROM artistscount" + testInstance;

        server.execute(create);
        List<String> result = server.execute(select);
        assertTrue("Hive returned null", containsNoNull(result));
        assertEquals(1, result.size());
        assertTrue(Integer.valueOf(result.get(0)) > 1);
    }

    @Test
    public void basicArrayMapping() throws Exception {
        String create = "CREATE EXTERNAL TABLE compoundarray" + testInstance + " ("
                + "rid      BIGINT, "
                + "mapids   ARRAY<BIGINT>, "
                + "rdata    MAP<STRING, STRING>) "
                + tableProps("hive/compound");

        String select = "SELECT * FROM compoundarray" + testInstance;

        server.execute(create);
        List<String> result = server.execute(select);
        assertTrue(result.size() > 1);
        assertTrue("Hive returned null", containsNoNull(result));
        assertContains(result, "400,401");
        assertContains(result, "{\"6\":");
    }

    //@Test
    public void basicTimestampLoad() throws Exception {
        String create = "CREATE EXTERNAL TABLE timestampload" + testInstance + " ("
                + "id       BIGINT, "
                + "date     TIMESTAMP, "
                + "name     STRING, "
                + "links    STRUCT<url:STRING, picture:STRING>) "
                + tableProps("hive/artiststimestamp");

        String select = "SELECT date FROM timestampload" + testInstance;
        String select2 = "SELECT unix_timestamp(), date FROM timestampload" + testInstance;

        String date = new SimpleDateFormat("yyyy-MM-dd").format(new Date());

        server.execute(create);
        List<String> result = server.execute(select);
        assertTrue(result.size() > 1);
        assertTrue("Hive returned null", containsNoNull(result));
        assertContains(result, date);

        result = server.execute(select2);
        assertTrue("Hive returned null", containsNoNull(result));
        assertTrue(result.size() > 1);
        assertContains(result, date);
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

    //@Test
    public void javaMethodInvocation() throws Exception {
        String create = "CREATE EXTERNAL TABLE methodInvocation" + testInstance + " ("
                + "id       BIGINT, "
                + "name     STRING, "
                + "links    STRUCT<url:STRING, picture:STRING>) "
                + tableProps("hive/artists");

        long currentTimeMillis = System.currentTimeMillis();

        String select = "SELECT java_method(\"java.lang.System\", \"currentTimeMillis\") FROM methodInvocation"  + testInstance + " LIMIT 5";

        server.execute(create);
        List<String> result = server.execute(select);
        assertTrue("Hive returned null", containsNoNull(result));
        assertContains(result, String.valueOf(currentTimeMillis).substring(0, 5));
    }

    @Test
    public void columnAliases() throws Exception {
        String create = "CREATE EXTERNAL TABLE aliasload" + testInstance + " ("
                + "dTE     TIMESTAMP, "
                + "Name     STRING, "
                + "links    STRUCT<uRl:STRING, pICture:STRING>) "
                + tableProps("hive/aliassave", "'es.mapping.names' = 'dTE:@timestamp, uRl:url_123'");

        String select = "SELECT * FROM aliasload" + testInstance;

        server.execute(create);
        List<String> result = server.execute(select);
        assertTrue("Hive returned null", containsNoNull(result));
        assertTrue(result.size() > 1);
        assertContains(result, "Marilyn");
        assertContains(result, "last.fm/music/MALICE");
        assertContains(result, "last.fm/serve/252/2181591.jpg");
    }

    @Test
    public void testMissingIndex() throws Exception {
        String create = "CREATE EXTERNAL TABLE missing" + testInstance + " ("
                + "dTE     TIMESTAMP, "
                + "Name     STRING, "
                + "links    STRUCT<uRl:STRING, pICture:STRING>) "
                + tableProps("foobar/missing", "'es.index.read.missing.as.empty' = 'true'");

        String select = "SELECT * FROM missing" + testInstance;

        server.execute(create);
        List<String> result = server.execute(select);
        assertEquals(0, result.size());
    }

    @Test(expected = SQLException.class)
    public void testSourceFieldCollision() throws Exception {

        String create = "CREATE EXTERNAL TABLE collisiontest" + testInstance + "("
                + "id         BIGINT, "
                + "name     STRING, "
                + "links     STRUCT<url:STRING, picture:STRING>) "
                + tableProps("hive/artists", "'es.read.source.filter' = 'name,links'");

        String select = "SELECT * FROM collisiontest" + testInstance;

        server.execute(create);
        server.execute(select);
        fail("Should not have executed successfully: User specified source filter should conflict with source filter from connector.");
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
        assertTrue("Hive returned null", containsNoNull(result));
        assertTrue(result.size() > 1);
        assertContains(result, "Marilyn");
        assertContains(result, "last.fm/music/MALICE");
        assertContains(result, "last.fm/serve/252/2181591.jpg");
    }

    @Test
    public void testCharLoad() throws Exception {
        // create external table
        String create =
                "CREATE EXTERNAL TABLE charload" + testInstance + " ("
                + "id       BIGINT, "
                + "name     STRING, "
                + "links    STRUCT<url:STRING, picture:STRING>) "
                + tableProps("hive/charsave");

        // this does not
        String select = "SELECT * FROM charload" + testInstance;

        server.execute(create);
        List<String> result = server.execute(select);
        assertTrue(result.size() > 1);
        assertContains(result, "Marilyn");
        assertContains(result, "last.fm/music/MALICE");
        assertContains(result, "last.fm/serve/252/2181591.jpg");
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
        assertTrue(result.size() > 1);
        assertContains(result, "Marilyn");
        assertContains(result, "last.fm/music/MALICE");
        assertContains(result, "last.fm/serve/252/2181591.jpg");
    }


    @Test
    public void testReadWriteSameJob() throws Exception {

        String write =
                "CREATE EXTERNAL TABLE rwwrite" + testInstance +" ("
                + "id       BIGINT, "
                + "name     STRING, "
                + "links    STRUCT<url:STRING, picture:STRING>) "
                + tableProps("hive/rwwrite");


        String read =
                "CREATE EXTERNAL TABLE rwread" + testInstance +" ("
                + "id       BIGINT, "
                + "name     STRING, "
                + "links    STRUCT<url:STRING, picture:STRING>) "
                + tableProps("hive/artists");

        String selectInsert = "INSERT OVERWRITE TABLE rwwrite" + testInstance + " SELECT * FROM rwread" + testInstance;
        String select = "SELECT * FROM rwwrite" + testInstance;

        System.out.println(server.execute(read));
        System.out.println(server.execute(write));

        System.out.println(server.execute(selectInsert));
        List<String> result = server.execute(select);
        assertTrue("Hive returned null", containsNoNull(result));
        assertTrue(result.size() > 1);
        assertContains(result, "Marilyn");
        assertContains(result, "last.fm/music/MALICE");
        assertContains(result, "last.fm/serve/252/2181591.jpg");
    }

    //@Test
    public void basicJoin() throws Exception {

        String left = "CREATE EXTERNAL TABLE left" + testInstance + "("
                + "id       BIGINT, "
                + "name     STRING, "
                + "links    STRUCT<url:STRING, picture:STRING>) "
                + tableProps("hive/artists");

        String right = "CREATE EXTERNAL TABLE right" + testInstance + "("
                + "id       BIGINT, "
                + "name     STRING, "
                + "links    STRUCT<url:STRING, picture:STRING>) "
                + tableProps("hive/artists");

        String select = "SELECT * FROM left" + testInstance + " l JOIN right" + testInstance + " r ON l.id = r.id";
        //String select = "SELECT * FROM left" + testInstance + " l JOIN source r ON l.id = r.id";

        server.execute(left);
        server.execute(right);
        System.out.println(server.execute("SHOW CREATE TABLE left" + testInstance));
        System.out.println(server.execute("SHOW CREATE TABLE right" + testInstance));

        List<String> result = server.execute(select);
        assertTrue("Hive returned null", containsNoNull(result));
        assertContains(result, "Marilyn");
        assertContains(result, "last.fm/music/MALICE");
        assertContains(result, "last.fm/serve/252/5872875.jpg");
    }

    @Test
    public void testDynamicPattern() throws Exception {
        Assert.assertTrue(RestUtils.exists("hive/pattern-7"));
        Assert.assertTrue(RestUtils.exists("hive/pattern-10"));
        Assert.assertTrue(RestUtils.exists("hive/pattern-15"));
    }

    @Test
    public void testDynamicPatternFormat() throws Exception {
        Assert.assertTrue(RestUtils.exists("hive/pattern-format-2007-10-06"));
        Assert.assertTrue(RestUtils.exists("hive/pattern-format-2011-10-06"));
        Assert.assertTrue(RestUtils.exists("hive/pattern-format-2001-10-06"));
    }


    private static boolean containsNoNull(List<String> str) {
        for (String string : str) {
            if (string.contains("NULL")) {
                return false;
            }
        }

        return true;
    }

    private static void assertContains(List<String> str, String content) {
        for (String string : str) {
            if (string.contains(content)) {
                return;
            }
        }
        fail(String.format("'%s' not found in %s", content, str));
    }


    private String tableProps(String resource, String... params) {
        List<String> copy = new ArrayList(Arrays.asList(params));
        copy.add("'" + ConfigurationOptions.ES_READ_METADATA + "'='" + readMetadata + "'");
        return HiveSuite.tableProps(resource, query, copy.toArray(new String[copy.size()]));
    }
}