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

import java.util.Date;

import org.elasticsearch.hadoop.EsHadoopIllegalStateException;
import org.elasticsearch.hadoop.Provisioner;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.mr.RestUtils;
import org.elasticsearch.hadoop.rest.RestClient;
import org.elasticsearch.hadoop.util.EsMajorVersion;
import org.elasticsearch.hadoop.util.StringUtils;
import org.elasticsearch.hadoop.util.TestSettings;
import org.elasticsearch.hadoop.util.TestUtils;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import static org.elasticsearch.hadoop.util.EsMajorVersion.V_5_X;
import static org.junit.Assert.*;

import static org.hamcrest.CoreMatchers.*;

/**
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class AbstractPigSaveTest extends AbstractPigTests {

    private final EsMajorVersion VERSION = TestUtils.getEsVersion();

    @BeforeClass
    public static void localStartup() throws Exception {
        AbstractPigTests.startup();

        // initialize Pig in local mode
        RestClient client = new RestClient(new TestSettings());
        try {
            client.delete("pig");
        } catch (Exception ex) {
            // ignore
        }
    }

    @Test
    public void testTuple() throws Exception {
        String script =
                "SET mapred.map.tasks 2;" +
                "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                // "A = LOAD 'src/test/resources/artists.dat' USING PigStorage() AS (id:long, name:chararray, links:tuple(url:chararray, picture: chararray));" +
                loadArtistSource() +
                //"ILLUSTRATE A;" +
                "B = FOREACH A GENERATE name, TOTUPLE(url, picture) AS links;" +
                "DESCRIBE B;" +
                "ILLUSTRATE B;" +
                "STORE B INTO 'pig-tupleartists/data' USING org.elasticsearch.hadoop.pig.EsStorage();";
        //"es_total = LOAD 'radio/artists/_count?q=me*' USING org.elasticsearch.hadoop.pig.EsStorage();" +
        //"DUMP es_total;" +
        //"bartists = FILTER B BY name MATCHES 'me.*';" +
        //"allb = GROUP bartists ALL;"+
        //"total = FOREACH allb GENERATE 'total' as foo, COUNT_STAR($1) as total;"+
        //"ILLUSTRATE allb;"+
        //"STORE total INTO '/tmp/total';"+
        //"DUMP total;";
        pig.executeScript(script);
    }

    @Test
    public void testTupleMapping() throws Exception {
        assertThat(RestUtils.getMapping("pig-tupleartists/data").toString(),
                VERSION.onOrAfter(V_5_X)
                        ? is("data=[links=TEXT, name=TEXT]")
                        : is("data=[links=STRING, name=STRING]"));
    }

    @Test
    public void testBag() throws Exception {
        String script =
                "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                //"A = LOAD 'src/test/resources/artists.dat' USING PigStorage() AS (id:long, name, links:bag{t:(url:chararray, picture: chararray)});" +
                loadArtistSource() +
                "B = FOREACH A GENERATE name, TOBAG(url, picture) AS links;" +
                "ILLUSTRATE B;" +
                "STORE B INTO 'pig-bagartists/data' USING org.elasticsearch.hadoop.pig.EsStorage();";
        pig.executeScript(script);
    }

    @Test
    public void testBagMapping() throws Exception {
        assertThat(RestUtils.getMapping("pig-bagartists/data").toString(),
                VERSION.onOrAfter(V_5_X)
                        ? is("data=[links=TEXT, name=TEXT]")
                        : is("data=[links=STRING, name=STRING]"));
    }

    @Test
    public void testTimestamp() throws Exception {
        long millis = new Date().getTime();
        String script =
                "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                //"A = LOAD 'src/test/resources/artists.dat' USING PigStorage() AS (id:long, name, links:bag{t:(url:chararray, picture: chararray)});" +
                loadArtistSource() +
                "B = FOREACH A GENERATE name, ToDate(" + millis + "l) AS date, url;" +
                "ILLUSTRATE B;" +
                "STORE B INTO 'pig-timestamp/data' USING org.elasticsearch.hadoop.pig.EsStorage();";

        pig.executeScript(script);
    }

    @Test
    public void testTimestampMapping() throws Exception {
        String mapping = RestUtils.getMapping("pig-timestamp/data").toString();
        assertThat(mapping, containsString("date=DATE"));
    }

    @Test
    public void testFieldAlias() throws Exception {
        long millis = new Date().getTime();
        String script =
                "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                //"A = LOAD 'src/test/resources/artists.dat' USING PigStorage() AS (id:long, name, links:bag{t:(url:chararray, picture: chararray)});" +
                loadArtistSource() +
                "B = FOREACH A GENERATE name, ToDate(" + millis + "l) AS timestamp, url, picture;" +
                "ILLUSTRATE B;" +
                "STORE B INTO 'pig-fieldalias/data' USING org.elasticsearch.hadoop.pig.EsStorage('es.mapping.names=nAme:@name, timestamp:@timestamp, uRL:url, picturE:picture');";

        pig.executeScript(script);
    }

    @Test
    public void testFieldAliasMapping() throws Exception {
        assertThat(RestUtils.getMapping("pig-fieldalias/data").toString(),
                VERSION.onOrAfter(V_5_X)
                        ? is("data=[@timestamp=DATE, name=TEXT, picture=TEXT, url=TEXT]")
                        : is("data=[@timestamp=DATE, name=STRING, picture=STRING, url=STRING]"));
    }

    @Test
    public void testCaseSensitivity() throws Exception {
        String script =
                "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                //"A = LOAD 'src/test/resources/artists.dat' USING PigStorage() AS (id:long, name, links:bag{t:(url:chararray, picture: chararray)});" +
                "A = LOAD '" + TestUtils.sampleArtistsDat() + "' USING PigStorage() AS (id:long, Name:chararray, uRL:chararray, pIctUre: chararray, timestamp: chararray); " +
                "B = FOREACH A GENERATE Name, uRL, pIctUre;" +
                "ILLUSTRATE B;" +
                "STORE B INTO 'pig-casesensitivity/data' USING org.elasticsearch.hadoop.pig.EsStorage();";

        pig.executeScript(script);
    }

    @Test
    public void testCaseSensitivityMapping() throws Exception {
        assertThat(RestUtils.getMapping("pig-casesensitivity/data").toString(),
                VERSION.onOrAfter(V_5_X)
                        ? is("data=[Name=TEXT, pIctUre=TEXT, uRL=TEXT]")
                        : is("data=[Name=STRING, pIctUre=STRING, uRL=STRING]"));
    }

    @Test
    public void testEmptyComplexStructures() throws Exception {
        String script =
                "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                loadArtistSource() +
                "AL = LIMIT A 10;" +
                "B = FOREACH AL GENERATE (), [], {};" +
                "STORE B INTO 'pig-emptyconst/data' USING org.elasticsearch.hadoop.pig.EsStorage();";

        pig.executeScript(script);
    }

    @Test
    public void testEmptyComplexStructuresMapping() throws Exception {
        assertThat(RestUtils.getMapping("pig-emptyconst/data").toString(), is("data=[]"));
    }

    @Test
    public void testCreateWithId() throws Exception {
        String script =
                "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                loadArtistSource() +
                "B = FOREACH A GENERATE id, name, TOBAG(url, picture) AS links;" +
                "STORE B INTO 'pig-createwithid/data' USING org.elasticsearch.hadoop.pig.EsStorage('"
                                + ConfigurationOptions.ES_WRITE_OPERATION + "=create','"
                                + ConfigurationOptions.ES_MAPPING_ID + "=id');";
        pig.executeScript(script);
    }

    @Test
    public void testCreateWithIdMapping() throws Exception {
        assertThat(RestUtils.getMapping("pig-createwithid/data").toString(),
                VERSION.onOrAfter(V_5_X)
                        ? is("data=[id=LONG, links=TEXT, name=TEXT]")
                        : is("data=[id=LONG, links=STRING, name=STRING]"));
    }

    @Test(expected = EsHadoopIllegalStateException.class)
    public void testCreateWithIdShouldFailOnDuplicate() throws Exception {
        testCreateWithId();
    }

    @Test(expected = Exception.class)
    public void testUpdateWithoutId() throws Exception {
        String script =
                "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                loadArtistSource() +
                "B = FOREACH A GENERATE id, name, TOBAG(url, picture) AS links;" +
                "STORE B INTO 'pig-updatewoid/data' USING org.elasticsearch.hadoop.pig.EsStorage('"
                                + ConfigurationOptions.ES_WRITE_OPERATION + "=update');";
        pig.executeScript(script);
    }

    @Test
    public void testUpdateWithId() throws Exception {
        String script =
                "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                loadArtistSource() +
                "B = FOREACH A GENERATE id, name, TOBAG(url, picture) AS links;" +
                "STORE B INTO 'pig-update/data' USING org.elasticsearch.hadoop.pig.EsStorage('"
                                + ConfigurationOptions.ES_WRITE_OPERATION + "=upsert','"
                                + ConfigurationOptions.ES_MAPPING_ID + "=id');";
        pig.executeScript(script);
    }

    @Test
    public void testUpdateWithIdMapping() throws Exception {
        assertThat(RestUtils.getMapping("pig-update/data").toString(),
                VERSION.onOrAfter(V_5_X)
                        ? is("data=[id=LONG, links=TEXT, name=TEXT]")
                        : is("data=[id=LONG, links=STRING, name=STRING]"));
    }

    @Test(expected = EsHadoopIllegalStateException.class)
    public void testUpdateWithoutUpsert() throws Exception {
        String script =
                "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                loadArtistSource() +
                "B = FOREACH A GENERATE id, name, TOBAG(url, picture) AS links;" +
                "STORE B INTO 'pig-updatewoupsert/data' USING org.elasticsearch.hadoop.pig.EsStorage('"
                                + ConfigurationOptions.ES_WRITE_OPERATION + "=update','"
                                + ConfigurationOptions.ES_MAPPING_ID + "=id');";
        pig.executeScript(script);
    }

    @Test
    public void testParentChild() throws Exception {
        RestUtils.putMapping("pig-pc/child", "org/elasticsearch/hadoop/integration/mr-child.json");

        String script =
                "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                loadArtistSource() +
                "B = FOREACH A GENERATE id, name, TOBAG(url, picture) AS links;" +
                "STORE B INTO 'pig-pc/child' USING org.elasticsearch.hadoop.pig.EsStorage('"
                                + ConfigurationOptions.ES_MAPPING_PARENT + "=id','"
                                + ConfigurationOptions.ES_INDEX_AUTO_CREATE + "=no');";
        pig.executeScript(script);
    }

    @Test
    public void testParentChildMapping() throws Exception {
        assertThat(RestUtils.getMapping("pig-pc/child").toString(),
                VERSION.onOrAfter(V_5_X)
                        ? is("child=[id=LONG, links=TEXT, name=TEXT]")
                        : is("child=[id=LONG, links=STRING, name=STRING]"));
    }

    @Test
    @Ignore("Pig can't really create objects for insertion, so transforming this " +
            "data field into a joiner is bunk right now. Fix this when we figure " +
            "out how to handle tuples well in Pig...")
    public void testJoin() throws Exception {
        RestUtils.putMapping("pig-join", "join", "data/join/mapping.json");
        RestUtils.refresh("pig-join");

        String script =
                "REGISTER " + Provisioner.ESHADOOP_TESTING_JAR + ";" +
                        loadJoinSource()
                        + "CHILDREN = FILTER A BY NOT(parent IS NULL);"
//                        + "CHILDREN = FOREACH A GENERATE id, name, company, TOTUPLE(relation, parent) as joiner;"
                        + "DUMP A;"
                        + "DUMP CHILDREN;"
//                        + "STORE CHILDREN INTO 'pig-join/join' USING org.elasticsearch.hadoop.pig.EsStorage('"
//                        + ConfigurationOptions.ES_MAPPING_JOIN + "=joiner','"
//                        + ConfigurationOptions.ES_INDEX_AUTO_CREATE + "=no');"
                ;
        pig.executeScript(script);
    }

    @Test
    public void testNestedTuple() throws Exception {
        RestUtils.postData("pig-nestedtuple/data", "{\"my_array\" : [\"1.a\",\"1.b\"]}".getBytes(StringUtils.UTF_8));
        RestUtils.postData("pig-nestedtuple/data", "{\"my_array\" : [\"2.a\",\"2.b\"]}".getBytes(StringUtils.UTF_8));
        RestUtils.waitForYellow("pig-nestedtuple");
    }


    @Test
    public void testIndexPattern() throws Exception {
        String script =
                "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                loadArtistSource() +
                "STORE A INTO 'pig-pattern-{tag}/data' USING org.elasticsearch.hadoop.pig.EsStorage();";

        pig.executeScript(script);
    }

    @Test
    public void testIndexPatternMapping() throws Exception {
        assertThat(RestUtils.getMapping("pig-pattern-9/data").toString(),
                VERSION.onOrAfter(V_5_X)
                        ? is("data=[id=LONG, name=TEXT, picture=TEXT, tag=LONG, timestamp=DATE, url=TEXT]")
                        : is("data=[id=LONG, name=STRING, picture=STRING, tag=LONG, timestamp=DATE, url=STRING]"));
    }

    @Test
    public void testIndexPatternFormat() throws Exception {
        String script =
                "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                loadArtistSource() +
                "STORE A INTO 'pig-pattern-format-{timestamp|YYYY-MM-dd}/data' USING org.elasticsearch.hadoop.pig.EsStorage();";

        pig.executeScript(script);
    }

    @Test
    public void testIndexPatternFormatMapping() throws Exception {
        assertThat(RestUtils.getMapping("pig-pattern-format-2001-10-06/data").toString(),
                VERSION.onOrAfter(V_5_X)
                        ? is("data=[id=LONG, name=TEXT, picture=TEXT, tag=LONG, timestamp=DATE, url=TEXT]")
                        : is("data=[id=LONG, name=STRING, picture=STRING, tag=LONG, timestamp=DATE, url=STRING]"));
    }

    private String loadArtistSource() {
        return loadSource(TestUtils.sampleArtistsDat()) + " AS (id:long, name:chararray, url:chararray, picture: chararray, timestamp: chararray, tag:long);";
    }

    private String loadJoinSource() {
        return loadSource(TestUtils.sampleJoinDat()) + " AS (id:long, company:chararray, name:chararray, relation:chararray, parent:chararray);";
    }

    private String loadSource(String source) {
        return "A = LOAD '" + source + "' USING PigStorage()";
    }
}