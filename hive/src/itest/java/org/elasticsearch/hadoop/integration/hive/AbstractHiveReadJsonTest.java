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

import org.apache.hive.service.cli.HiveSQLException;
import org.elasticsearch.hadoop.QueryTestParams;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.mr.EsAssume;
import org.elasticsearch.hadoop.mr.RestUtils;
import org.elasticsearch.hadoop.util.EsMajorVersion;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.hadoop.integration.hive.HiveSuite.provisionEsLib;
import static org.elasticsearch.hadoop.integration.hive.HiveSuite.server;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

@SuppressWarnings("Duplicates")
@RunWith(Parameterized.class)
public class AbstractHiveReadJsonTest {

    private static int testInstance = 0;
    private final boolean readMetadata;

    @Parameters
    public static Collection<Object[]> queries() {
        return QueryTestParams.params();
    }

    private final String query;

    public AbstractHiveReadJsonTest(String query, boolean readMetadata) {
        this.query = query;
        this.readMetadata = readMetadata;
    }

    @Before
    public void before() throws Exception {
        provisionEsLib();
        RestUtils.refresh("json-hive*");
    }

    @After
    public void after() throws Exception {
        testInstance++;
        HiveSuite.after();
    }

    @Test
    public void basicLoad() throws Exception {

        String create = "CREATE EXTERNAL TABLE jsonartistsread" + testInstance + " (data INT, garbage INT, garbage2 STRING) "
                + tableProps("json-hive-artists/data", "'es.output.json' = 'true'", "'es.mapping.names'='garbage2:refuse'");

        String select = "SELECT * FROM jsonartistsread" + testInstance;

        server.execute(create);
        List<String> result = server.execute(select);
        assertTrue("Hive returned null", containsNoNull(result));
        assertContains(result, "Marilyn");
        assertContains(result, "last.fm/music/MALICE");
        assertContains(result, "last.fm/serve/252/5872875.jpg");
    }

    @Test
    public void basicLoadWithNameMappings() throws Exception {

        String create = "CREATE EXTERNAL TABLE jsonartistsread" + testInstance + " (refuse INT, garbage INT, data STRING) "
                + tableProps("json-hive-artists/data", "'es.output.json' = 'true'", "'es.mapping.names'='data:boomSomethingYouWerentExpecting'");

        String select = "SELECT * FROM jsonartistsread" + testInstance;

        server.execute(create);
        List<String> result = server.execute(select);
        assertTrue("Hive returned null", containsNoNull(result));
        assertContains(result, "Marilyn");
        assertContains(result, "last.fm/music/MALICE");
        assertContains(result, "last.fm/serve/252/5872875.jpg");
    }

    @Test(expected = HiveSQLException.class)
    public void basicLoadWithNoGoodCandidateField() throws Exception {

        String create = "CREATE EXTERNAL TABLE jsonartistsread" + testInstance + " (refuse INT, garbage INT) "
                + tableProps("json-hive-artists/data", "'es.output.json' = 'true'");

        String select = "SELECT * FROM jsonartistsread" + testInstance;

        server.execute(create);
        server.execute(select);

        fail("Should have broken because there are no String fields in the table schema to place the JSON data.");
    }

    @Test
    public void testMissingIndex() throws Exception {
        String create = "CREATE EXTERNAL TABLE jsonmissingread" + testInstance + " (data STRING) "
                + tableProps("foobar/missing", "'es.index.read.missing.as.empty' = 'true'", "'es.output.json' = 'true'");

        String select = "SELECT * FROM jsonmissingread" + testInstance;

        server.execute(create);
        List<String> result = server.execute(select);
        assertEquals(0, result.size());
    }

    @Test
    public void testParentChild() throws Exception {
        EsAssume.versionOnOrBefore(EsMajorVersion.V_5_X, "Parent Child Disabled in 6.0");
        String create = "CREATE EXTERNAL TABLE jsonchildread" + testInstance + " (data STRING) "
                + tableProps("json-hive-pc/child", "'es.index.read.missing.as.empty' = 'true'", "'es.output.json' = 'true'");

        String select = "SELECT * FROM jsonchildread" + testInstance;

        System.out.println(server.execute(create));
        List<String> result = server.execute(select);
        assertTrue("Hive returned null", containsNoNull(result));
        assertTrue(result.size() > 1);
        assertContains(result, "Marilyn");
        assertContains(result, "last.fm/music/MALICE");
        assertContains(result, "last.fm/serve/252/2181591.jpg");
    }

    @Test
    public void testNoSourceFilterCollisions() throws Exception {

        String create = "CREATE EXTERNAL TABLE jsonartistscollisionread" + testInstance + " (data INT, garbage INT, garbage2 STRING) "
                + tableProps(
                    "json-hive-artists/data",
                    "'es.output.json' = 'true'",
                    "'es.read.source.filter'='name'"
                );

        String select = "SELECT * FROM jsonartistscollisionread" + testInstance;

        server.execute(create);
        List<String> result = server.execute(select);
        assertTrue("Hive returned null", containsNoNull(result));
        System.out.println(result);
        assertContains(result, "Marilyn");
        assertThat(result, not(hasItem(containsString("last.fm/music/MALICE"))));
        assertThat(result, not(hasItem(containsString("last.fm/serve/252/5872875.jpg"))));
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