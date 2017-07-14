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

package org.elasticsearch.hadoop.integration.pig;

import com.google.common.collect.Lists;
import org.elasticsearch.hadoop.Provisioner;
import org.elasticsearch.hadoop.QueryTestParams;
import org.elasticsearch.hadoop.mr.RestUtils;
import org.elasticsearch.hadoop.util.EsMajorVersion;
import org.elasticsearch.hadoop.util.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.stringContainsInOrder;
import static org.junit.Assert.assertThat;

@RunWith(Parameterized.class)
public class AbstractPigReadAsJsonTest extends AbstractPigTests {

    private static int testInstance = 0;
    private static String previousQuery;
    private boolean readMetadata;
    private EsMajorVersion testVersion;

    @Parameters
    public static Collection<Object[]> queries() {
        return QueryTestParams.params();
    }

    private final String query;

    public AbstractPigReadAsJsonTest(String query, boolean metadata) {
        this.query = query;
        this.readMetadata = metadata;
        this.testVersion = TestUtils.getEsVersion();

        if (!query.equals(previousQuery)) {
            previousQuery = query;
            testInstance++;
        }
    }

    private String scriptHead;

    @Before
    public void before() throws Exception {
        RestUtils.refresh("json-pig*");

        this.scriptHead = "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                "DEFINE EsStorage org.elasticsearch.hadoop.pig.EsStorage('es.index.read.missing.as.empty=true','es.query=" + query + "','es.read.metadata=" + readMetadata +"','es.output.json=true');";
    }

    @Test
    public void testTuple() throws Exception {
        String script = scriptHead +
                "A = LOAD 'json-pig-tupleartists/data' USING EsStorage();" +
                "X = LIMIT A 3;" +
                //"DESCRIBE A;";
                "STORE A INTO '" + tmpPig() + "/testtuple';";
        pig.executeScript(script);

        String results = getResults("" + tmpPig() + "/testtuple");

        List<String> doc1 = Lists.newArrayList(
                "{\"number\":\"12\",\"name\":\"Behemoth\",\"url\":\"http://www.last.fm/music/Behemoth\",\"picture\":\"http://userserve-ak.last.fm/serve/252/54196161.jpg\",\"@timestamp\":\"2001-10-06T19:20:25.000Z\",\"list\":[\"quick\", \"brown\", \"fox\"]"
        );
        if (readMetadata) {
            doc1.add(",\"_metadata\":{\"_index\":\"json-pig-tupleartists\",\"_type\":\"data\",\"_id\":\"");
            doc1.add("\",\"_score\":");
        }

        List<String> doc2 = Lists.newArrayList(
                "{\"number\":\"918\",\"name\":\"Megadeth\",\"url\":\"http://www.last.fm/music/Megadeth\",\"picture\":\"http://userserve-ak.last.fm/serve/252/8129787.jpg\",\"@timestamp\":\"2017-10-06T19:20:25.000Z\",\"list\":[\"quick\", \"brown\", \"fox\"]"
        );
        if (readMetadata) {
            doc2.add(",\"_metadata\":{\"_index\":\"json-pig-tupleartists\",\"_type\":\"data\",\"_id\":\"");
            doc2.add("\",\"_score\":");
        }

        List<String> doc3 = Lists.newArrayList(
                "{\"number\":\"982\",\"name\":\"Foo Fighters\",\"url\":\"http://www.last.fm/music/Foo+Fighters\",\"picture\":\"http://userserve-ak.last.fm/serve/252/59495563.jpg\",\"@timestamp\":\"2017-10-06T19:20:25.000Z\",\"list\":[\"quick\", \"brown\", \"fox\"]"
        );
        if (readMetadata) {
            doc3.add(",\"_metadata\":{\"_index\":\"json-pig-tupleartists\",\"_type\":\"data\",\"_id\":\"");
            doc3.add("\",\"_score\":");
        }

        assertThat(results, stringContainsInOrder(doc1));
        assertThat(results, stringContainsInOrder(doc2));
        assertThat(results, stringContainsInOrder(doc3));
    }

    @Test
    public void testTupleWithSchema() throws Exception {
        String script = scriptHead +
                "A = LOAD 'json-pig-tupleartists/data' USING EsStorage() AS (name:chararray);" +
                "B = ORDER A BY name DESC;" +
                "X = LIMIT B 3;" +
                "STORE B INTO '" + tmpPig() + "/testtupleschema';";
        pig.executeScript(script);

        String results = getResults("" + tmpPig() + "/testtupleschema");

        List<String> doc1 = Lists.newArrayList(
                "{\"number\":\"999\",\"name\":\"Thompson Twins\",\"url\":\"http://www.last.fm/music/Thompson+Twins\",\"picture\":\"http://userserve-ak.last.fm/serve/252/6943589.jpg\",\"@timestamp\":\"2017-10-06T19:20:25.000Z\",\"list\":[\"quick\", \"brown\", \"fox\"]"
        );
        if (readMetadata) {
            doc1.add(",\"_metadata\":{\"_index\":\"json-pig-tupleartists\",\"_type\":\"data\",\"_id\":\"");
            doc1.add("\",\"_score\":");
        }

        List<String> doc2 = Lists.newArrayList(
                "{\"number\":\"12\",\"name\":\"Behemoth\",\"url\":\"http://www.last.fm/music/Behemoth\",\"picture\":\"http://userserve-ak.last.fm/serve/252/54196161.jpg\",\"@timestamp\":\"2001-10-06T19:20:25.000Z\",\"list\":[\"quick\", \"brown\", \"fox\"]"
        );
        if (readMetadata) {
            doc2.add(",\"_metadata\":{\"_index\":\"json-pig-tupleartists\",\"_type\":\"data\",\"_id\":\"");
            doc2.add("\",\"_score\":");
        }

        List<String> doc3 = Lists.newArrayList(
                "{\"number\":\"230\",\"name\":\"Green Day\",\"url\":\"http://www.last.fm/music/Green+Day\",\"picture\":\"http://userserve-ak.last.fm/serve/252/15291249.jpg\",\"@timestamp\":\"2005-10-06T19:20:25.000Z\",\"list\":[\"quick\", \"brown\", \"fox\"]"
        );
        if (readMetadata) {
            doc3.add(",\"_metadata\":{\"_index\":\"json-pig-tupleartists\",\"_type\":\"data\",\"_id\":\"");
            doc3.add("\",\"_score\":");
        }

        assertThat(results, stringContainsInOrder(doc1));
        assertThat(results, stringContainsInOrder(doc2));
        assertThat(results, stringContainsInOrder(doc3));
    }

    @Test
    public void testFieldAlias() throws Exception {
        String script = scriptHead
                      + "A = LOAD 'json-pig-fieldalias/data' USING EsStorage();"
                      + "X = LIMIT A 3;"
                      + "STORE A INTO '" + tmpPig() + "/testfieldalias';";
        pig.executeScript(script);

        String results = getResults("" + tmpPig() + "/testfieldalias");

        List<String> doc1 = Lists.newArrayList(
                "{\"number\":\"12\",\"name\":\"Behemoth\",\"url\":\"http://www.last.fm/music/Behemoth\",\"picture\":\"http://userserve-ak.last.fm/serve/252/54196161.jpg\",\"@timestamp\":\"2001-10-06T19:20:25.000Z\",\"list\":[\"quick\", \"brown\", \"fox\"]"
        );
        if (readMetadata) {
            doc1.add(",\"_metadata\":{\"_index\":\"json-pig-fieldalias\",\"_type\":\"data\",\"_id\":\"");
            doc1.add("\",\"_score\":");
        }

        List<String> doc2 = Lists.newArrayList(
                "{\"number\":\"918\",\"name\":\"Megadeth\",\"url\":\"http://www.last.fm/music/Megadeth\",\"picture\":\"http://userserve-ak.last.fm/serve/252/8129787.jpg\",\"@timestamp\":\"2017-10-06T19:20:25.000Z\",\"list\":[\"quick\", \"brown\", \"fox\"]"
        );
        if (readMetadata) {
            doc2.add(",\"_metadata\":{\"_index\":\"json-pig-fieldalias\",\"_type\":\"data\",\"_id\":\"");
            doc2.add("\",\"_score\":");
        }

        List<String> doc3 = Lists.newArrayList(
                "{\"number\":\"982\",\"name\":\"Foo Fighters\",\"url\":\"http://www.last.fm/music/Foo+Fighters\",\"picture\":\"http://userserve-ak.last.fm/serve/252/59495563.jpg\",\"@timestamp\":\"2017-10-06T19:20:25.000Z\",\"list\":[\"quick\", \"brown\", \"fox\"]"
        );
        if (readMetadata) {
            doc3.add(",\"_metadata\":{\"_index\":\"json-pig-fieldalias\",\"_type\":\"data\",\"_id\":\"");
            doc3.add("\",\"_score\":");
        }

        assertThat(results, stringContainsInOrder(doc1));
        assertThat(results, stringContainsInOrder(doc2));
        assertThat(results, stringContainsInOrder(doc3));
    }

    @Test
    public void testMissingIndex() throws Exception {
        String script = scriptHead
                      + "A = LOAD 'foo/bar' USING EsStorage();"
                      + "X = LIMIT A 3;"
                      + "STORE A INTO '" + tmpPig() + "/testmissingindex';";
        pig.executeScript(script);

        String results = getResults("" + tmpPig() + "/testmissingindex");
        assertThat(results.length(), is(0));
    }

    @Test
    public void testParentChild() throws Exception {
        String script = scriptHead
                      + "A = LOAD 'json-pig-pc/child' USING EsStorage();"
                      + "X = LIMIT A 3;"
                      + "STORE A INTO '" + tmpPig() + "/testparentchild';";
        pig.executeScript(script);

        String results = getResults("" + tmpPig() + "/testparentchild");

        List<String> doc1 = Lists.newArrayList(
                "{\"number\":\"12\",\"name\":\"Behemoth\",\"url\":\"http://www.last.fm/music/Behemoth\",\"picture\":\"http://userserve-ak.last.fm/serve/252/54196161.jpg\",\"@timestamp\":\"2001-10-06T19:20:25.000Z\",\"list\":[\"quick\", \"brown\", \"fox\"]"
        );
        if (readMetadata) {
            doc1.add(",\"_metadata\":{\"_index\":\"json-pig-pc\",\"_type\":\"child\",\"_id\":\"");
            doc1.add("\",\"_score\":");
            if (testVersion.onOrAfter(EsMajorVersion.V_2_X)) {
                doc1.add("\"_routing\":\"12\",\"_parent\":\"12\"");
            }
        }

        List<String> doc2 = Lists.newArrayList(
                "{\"number\":\"918\",\"name\":\"Megadeth\",\"url\":\"http://www.last.fm/music/Megadeth\",\"picture\":\"http://userserve-ak.last.fm/serve/252/8129787.jpg\",\"@timestamp\":\"2017-10-06T19:20:25.000Z\",\"list\":[\"quick\", \"brown\", \"fox\"]"
        );
        if (readMetadata) {
            doc2.add(",\"_metadata\":{\"_index\":\"json-pig-pc\",\"_type\":\"child\",\"_id\":\"");
            doc2.add("\",\"_score\":");
            if (testVersion.onOrAfter(EsMajorVersion.V_2_X)) {
                doc2.add("\"_routing\":\"918\",\"_parent\":\"918\"");
            }
        }

        List<String> doc3 = Lists.newArrayList(
                "{\"number\":\"982\",\"name\":\"Foo Fighters\",\"url\":\"http://www.last.fm/music/Foo+Fighters\",\"picture\":\"http://userserve-ak.last.fm/serve/252/59495563.jpg\",\"@timestamp\":\"2017-10-06T19:20:25.000Z\",\"list\":[\"quick\", \"brown\", \"fox\"]"
        );
        if (readMetadata) {
            doc3.add(",\"_metadata\":{\"_index\":\"json-pig-pc\",\"_type\":\"child\",\"_id\":\"");
            doc3.add("\",\"_score\":");
            if (testVersion.onOrAfter(EsMajorVersion.V_2_X)) {
                doc3.add("\"_routing\":\"982\",\"_parent\":\"982\"");
            }
        }

        assertThat(results, stringContainsInOrder(doc1));
        assertThat(results, stringContainsInOrder(doc2));
        assertThat(results, stringContainsInOrder(doc3));
    }

    @Test
    public void testDynamicPattern() throws Exception {
        Assert.assertTrue(RestUtils.exists("json-pig-pattern-1/data"));
        Assert.assertTrue(RestUtils.exists("json-pig-pattern-5/data"));
        Assert.assertTrue(RestUtils.exists("json-pig-pattern-9/data"));
    }

    @Test
    public void testDynamicPatternFormat() throws Exception {
        Assert.assertTrue(RestUtils.exists("json-pig-pattern-format-2001-10-06/data"));
        Assert.assertTrue(RestUtils.exists("json-pig-pattern-format-2005-10-06/data"));
        Assert.assertTrue(RestUtils.exists("json-pig-pattern-format-2017-10-06/data"));
    }

    private static String tmpPig() {
        return "tmp-pig/json-read-" + testInstance;
    }
}
