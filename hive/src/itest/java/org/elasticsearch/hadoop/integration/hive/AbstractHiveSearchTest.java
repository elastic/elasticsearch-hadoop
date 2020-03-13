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
import org.elasticsearch.hadoop.EsAssume;
import org.elasticsearch.hadoop.rest.RestUtils;
import org.elasticsearch.hadoop.util.EsMajorVersion;
import org.elasticsearch.hadoop.util.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.LazyTempFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static org.elasticsearch.hadoop.util.TestUtils.resource;
import static org.junit.Assert.*;

import static org.elasticsearch.hadoop.integration.hive.HiveSuite.*;

@RunWith(Parameterized.class)
public class AbstractHiveSearchTest {

    private static int testInstance = 0;

    @ClassRule
    public static LazyTempFolder tempFolder = new LazyTempFolder();

    @Parameters
    public static Collection<Object[]> queries() {
        return new QueryTestParams(tempFolder).params();
    }

    private String query;
    private boolean readMetadata;
    private EsMajorVersion targetVersion;

    public AbstractHiveSearchTest(String query, boolean readMetadata) {
        this.query = query;
        this.readMetadata = readMetadata;
    }


    @Before
    public void before() throws Exception {
        provisionEsLib();
        RestUtils.refresh("hive*");
        targetVersion = TestUtils.getEsClusterInfo().getMajorVersion();
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
                + tableProps(resource("hive-artists", "data", targetVersion));

        String select = "SELECT * FROM artistsload" + testInstance;

        server.execute(create);
        List<String> result = server.execute(select);
        assertTrue("Hive returned null", containsNoNull(result));
        assertContains(result, "Marilyn");
        assertContains(result, "last.fm/music/MALICE");
        assertContains(result, "last.fm/serve/252/5872875.jpg");
    }

    @Test
    public void basicLoadWMetadata() throws Exception {
        Assume.assumeTrue("Only applicable to metadata reading", readMetadata);
        String create = "CREATE EXTERNAL TABLE artistsload" + testInstance + "("
                + "id         BIGINT, "
                + "name     STRING, "
                + "links     STRUCT<url:STRING, picture:STRING>, "
                + "meta     MAP<STRING, STRING>) "
                + tableProps(resource("hive-artists", "data", targetVersion), "'es.read.metadata.field'='meta'");

        String select = "SELECT meta FROM artistsload" + testInstance;

        server.execute(create);
        List<String> result = server.execute(select);
        assertTrue("Hive returned null", containsNoNull(result));
        assertContains(result, "\"_score\":\"1.0\"");
        System.out.println(result);
    }

    //@Test
    public void basicCountOperator() throws Exception {
        String create = "CREATE EXTERNAL TABLE artistscount" + testInstance + " ("
                + "id       BIGINT, "
                + "name     STRING, "
                + "links    STRUCT<url:STRING, picture:STRING>) "
                + tableProps(resource("hive-artists", "data", targetVersion));

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
                + tableProps(resource("hive-compound", "data", targetVersion));

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
                + tableProps(resource("hive-artiststimestamp", "data", targetVersion));

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
                + tableProps(resource("hive-datesave", "data", targetVersion));

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
                + tableProps(resource("hive-artists", "data", targetVersion));

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
                + tableProps(resource("hive-aliassave", "data", targetVersion), "'es.mapping.names' = 'dTE:@timestamp, uRl:url_123'");

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
                + tableProps(resource("foobar", "missing", targetVersion), "'es.index.read.missing.as.empty' = 'true'");

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
                + tableProps(resource("hive-artists", "data", targetVersion), "'es.read.source.filter' = 'name,links'");

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
                + tableProps(resource("hive-varcharsave", "data", targetVersion));

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
        String create = "CREATE EXTERNAL TABLE charload" + testInstance + " ("
                + "id       BIGINT, "
                + "name     STRING, "
                + "links    STRUCT<url:STRING, picture:STRING>) "
                + tableProps(resource("hive-charsave", "data", targetVersion));

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
        EsAssume.versionOnOrBefore(EsMajorVersion.V_5_X, "Parent Child Disabled in 6.0");
        String create = "CREATE EXTERNAL TABLE childload" + testInstance + " ("
                + "id       BIGINT, "
                + "name     STRING, "
                + "links    STRUCT<url:STRING, picture:STRING>) "
                + tableProps("hive-pc/child", "'es.index.read.missing.as.empty' = 'true'");

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

        String write = "CREATE EXTERNAL TABLE rwwrite" + testInstance + " ("
                + "id       BIGINT, "
                + "name     STRING, "
                + "links    STRUCT<url:STRING, picture:STRING>) "
                + tableProps(resource("hive-rwwrite", "data", targetVersion));


        String read = "CREATE EXTERNAL TABLE rwread" + testInstance + " ("
                + "id       BIGINT, "
                + "name     STRING, "
                + "links    STRUCT<url:STRING, picture:STRING>) "
                + tableProps(resource("hive-artists", "data", targetVersion));

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
                + tableProps(resource("hive-artists", "data", targetVersion));

        String right = "CREATE EXTERNAL TABLE right" + testInstance + "("
                + "id       BIGINT, "
                + "name     STRING, "
                + "links    STRUCT<url:STRING, picture:STRING>) "
                + tableProps(resource("hive-artists", "data", targetVersion));

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
    public void basicUnion() throws Exception {
        //table unionA and table uinonB should be from difference es index/type
        String unionA = "CREATE EXTERNAL TABLE uniona" + testInstance + " ("
                + "id       BIGINT, "
                + "name     STRING, "
                + "links    STRUCT<url:STRING, picture:STRING>) "
                + tableProps(resource("hive-artists", "data", targetVersion));

        String unionB = "CREATE EXTERNAL TABLE unionb" + testInstance + " ("
                + "id       BIGINT, "
                + "name     STRING, "
                + "links    STRUCT<url:STRING, picture:STRING>) "
                + tableProps(resource("hive-varcharsave", "data", targetVersion));

        //create two external table
        server.execute(unionA);
        server.execute(unionB);

        // select alone
        String selectA = "SELECT id,name FROM uniona" + testInstance;
        String selectB = "SELECT id,name FROM unionb" + testInstance;

        List<String> resultA = server.execute(selectA);
        List<String> resultB = server.execute(selectB);

        //select union
        String selectUnion = selectA + " UNION ALL " + selectB;
        List<String> resultUnion = server.execute(selectUnion);

        System.out.println(server.execute("SHOW CREATE TABLE uniona" + testInstance));
        System.out.println(server.execute("SHOW CREATE TABLE unionb" + testInstance));
        assertTrue("Hive returned null", containsNoNull(resultA));
        assertTrue("Hive returned null", containsNoNull(resultB));
        assertTrue("Hive returned null", containsNoNull(resultUnion));
        //union all operation don't remove the same elements,
        //so the total is equal to the sum of all the subqueries.
        assertTrue(resultA.size() + resultB.size() == resultUnion.size());
    }

    @Test
    public void testDynamicPattern() throws Exception {
        Assert.assertTrue(RestUtils.exists(resource("hive-pattern-7", "data", targetVersion)));
        Assert.assertTrue(RestUtils.exists(resource("hive-pattern-10", "data", targetVersion)));
        Assert.assertTrue(RestUtils.exists(resource("hive-pattern-15", "data", targetVersion)));
    }

    @Test
    public void testDynamicPatternFormat() throws Exception {
        Assert.assertTrue(RestUtils.exists(resource("hive-pattern-format-2007-10-06", "data", targetVersion)));
        Assert.assertTrue(RestUtils.exists(resource("hive-pattern-format-2011-10-06", "data", targetVersion)));
        Assert.assertTrue(RestUtils.exists(resource("hive-pattern-format-2001-10-06", "data", targetVersion)));
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