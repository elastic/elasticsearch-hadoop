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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.elasticsearch.hadoop.QueryTestParams;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.mr.RestUtils;
import org.elasticsearch.hadoop.util.StringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static org.elasticsearch.hadoop.integration.hive.HiveSuite.provisionEsLib;
import static org.elasticsearch.hadoop.integration.hive.HiveSuite.server;

@RunWith(Parameterized.class)
public class AbstractHiveSearchJsonTest {

    private static int testInstance = 0;
    private final boolean readMetadata;

    @Parameters
    public static Collection<Object[]> queries() {
        return QueryTestParams.params();
    }

    private final String query;

    public AbstractHiveSearchJsonTest(String query, boolean readMetadata) {
        this.query = query;
        this.readMetadata = readMetadata;
    }

    @Before
    public void before() throws Exception {
        provisionEsLib();
        RestUtils.refresh("json-hive");
    }

    @After
    public void after() throws Exception {
        testInstance++;
        HiveSuite.after();
    }

    @Test
    public void loadMultiNestedField() throws Exception {
        Assume.assumeTrue(testInstance == 0);
        String data = "{ \"data\" : { \"map\" : { \"key\" : [ 10 20 ] } } }";
        RestUtils.postData("json-hive/nestedmap", StringUtils.toUTF(data));
        data = "{ \"data\" : { \"different\" : \"structure\" } } }";
        RestUtils.postData("json-hive/nestedmap", StringUtils.toUTF(data));

        RestUtils.refresh("json-hive");

        String createList = "CREATE EXTERNAL TABLE jsonnestedmaplistload" + testInstance + "("
                + "nested   ARRAY<INT>) "
                + tableProps("json-hive/nestedmap", "'es.mapping.names' = 'nested:data.map.key'");

        String selectList = "SELECT * FROM jsonnestedmaplistload" + testInstance;

        String createMap = "CREATE EXTERNAL TABLE jsonnestedmapmapload" + testInstance + "("
                + "nested   MAP<STRING,ARRAY<INT>>) "
                + tableProps("json-hive/nestedmap", "'es.mapping.names' = 'nested:data.map'");

        String selectMap = "SELECT * FROM jsonnestedmapmapload" + testInstance;

        server.execute(createList);
        List<String> result = server.execute(selectList);
        assertTrue("Hive returned null", containsNoNull(result));
        assertContains(result, "10");
        assertContains(result, "20");

        server.execute(createMap);
        result = server.execute(selectMap);
        assertTrue("Hive returned null", containsNoNull(result));
        assertContains(result, "key");
        assertContains(result, "10");
        assertContains(result, "20");
    }

    @Test
    public void loadSingleNestedField() throws Exception {
        Assume.assumeTrue(testInstance == 0);
        String data = "{ \"data\" : { \"single\" : { \"key\" : [ 10 ] } } }";
        RestUtils.postData("json-hive/nestedmap", StringUtils.toUTF(data));

        RestUtils.refresh("json-hive");

        String createList = "CREATE EXTERNAL TABLE jsonnestedsinglemaplistload" + testInstance + "("
                + "nested   ARRAY<INT>) "
                + tableProps("json-hive/nestedmap", "'es.mapping.names' = 'nested:data.single.key'");

        String selectList = "SELECT * FROM jsonnestedsinglemaplistload" + testInstance;

        String createMap = "CREATE EXTERNAL TABLE jsonnestedsinglemapmapload" + testInstance + "("
                + "nested   MAP<STRING,ARRAY<INT>>) "
                + tableProps("json-hive/nestedmap", "'es.mapping.names' = 'nested:data.single'");

        String selectMap = "SELECT * FROM jsonnestedsinglemapmapload" + testInstance;

        server.execute(createList);
        List<String> result = server.execute(selectList);
        assertTrue("Hive returned null", containsNoNull(result));
        assertContains(result, "10");

        server.execute(createMap);
        result = server.execute(selectMap);
        assertTrue("Hive returned null", containsNoNull(result));
        assertContains(result, "key");
        assertContains(result, "10");
    }


    @Test
    public void basicLoad() throws Exception {

        String create = "CREATE EXTERNAL TABLE jsonartistsload" + testInstance + "("
                + "number         STRING, "
                + "name     STRING, "
                + "url  STRING, "
                + "picture  STRING) "
                + tableProps("json-hive/artists");

        String select = "SELECT * FROM jsonartistsload" + testInstance;

        server.execute(create);
        List<String> result = server.execute(select);
        assertTrue("Hive returned null", containsNoNull(result));
        assertContains(result, "Marilyn");
        assertContains(result, "last.fm/music/MALICE");
        assertContains(result, "last.fm/serve/252/5872875.jpg");
    }

    //@Test
    public void basicCountOperator() throws Exception {
        String create = "CREATE EXTERNAL TABLE jsonartistscount" + testInstance + " ("
                + "number       STRING, "
                + "name     STRING, "
                + "url  STRING, "
                + "picture  STRING) "
                + tableProps("json-hive/artists");

        String select = "SELECT count(*) FROM jsonartistscount" + testInstance;

        server.execute(create);
        List<String> result = server.execute(select);
        assertTrue("Hive returned null", containsNoNull(result));
        assertEquals(1, result.size());
        assertTrue(Integer.valueOf(result.get(0)) > 1);
    }

    @Test
    public void testMissingIndex() throws Exception {
        String create = "CREATE EXTERNAL TABLE jsonmissing" + testInstance + " ("
                + "dTE     TIMESTAMP, "
                + "Name     STRING, "
                + "links    STRUCT<uRl:STRING, pICture:STRING>) "
                + tableProps("foobar/missing", "'es.index.read.missing.as.empty' = 'true'");

        String select = "SELECT * FROM jsonmissing" + testInstance;

        server.execute(create);
        List<String> result = server.execute(select);
        assertEquals(0, result.size());
    }

    @Test
    public void testVarcharLoad() throws Exception {

        String create = "CREATE EXTERNAL TABLE jsonvarcharload" + testInstance + " ("
                + "number       STRING, "
                + "name     STRING, "
                + "url  STRING, "
                + "picture  STRING) "
                + tableProps("json-hive/varcharsave");

        String select = "SELECT * FROM jsonvarcharload" + testInstance;

        System.out.println(server.execute(create));
        List<String> result = server.execute(select);
        assertTrue("Hive returned null", containsNoNull(result));
        assertTrue(result.size() > 1);
        assertContains(result, "Marilyn");
        assertContains(result, "last.fm/music/MALICE");
        assertContains(result, "last.fm/serve/252/2181591.jpg");

    }

    @Test
    public void testParentChild() throws Exception {
        String create = "CREATE EXTERNAL TABLE jsonchildload" + testInstance + " ("
                + "number       STRING, "
                + "name     STRING, "
                + "url  STRING, "
                + "picture  STRING) "
                + tableProps("json-hive/child", "'es.index.read.missing.as.empty' = 'true'");

        String select = "SELECT * FROM jsonchildload" + testInstance;

        System.out.println(server.execute(create));
        List<String> result = server.execute(select);
        assertTrue("Hive returned null", containsNoNull(result));
        assertTrue(result.size() > 1);
        assertContains(result, "Marilyn");
        assertContains(result, "last.fm/music/MALICE");
        assertContains(result, "last.fm/serve/252/2181591.jpg");
    }

    @Test
    public void testDynamicPattern() throws Exception {
        Assert.assertTrue(RestUtils.exists("json-hive/pattern-7"));
        Assert.assertTrue(RestUtils.exists("json-hive/pattern-10"));
        Assert.assertTrue(RestUtils.exists("json-hive/pattern-3"));
    }

    @Test
    public void testDynamicPatternFormat() throws Exception {
        Assert.assertTrue(RestUtils.exists("json-hive/pattern-format-2007-10-06"));
        Assert.assertTrue(RestUtils.exists("json-hive/pattern-format-2001-10-06"));
        Assert.assertTrue(RestUtils.exists("json-hive/pattern-format-2000-10-06"));
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