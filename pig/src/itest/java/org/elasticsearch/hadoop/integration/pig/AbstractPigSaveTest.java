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
import org.elasticsearch.hadoop.util.StringUtils;
import org.elasticsearch.hadoop.util.TestSettings;
import org.elasticsearch.hadoop.util.TestUtils;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import static org.junit.Assert.*;

import static org.hamcrest.CoreMatchers.*;

/**
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class AbstractPigSaveTest extends AbstractPigTests {

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
                loadSource() +
                //"ILLUSTRATE A;" +
                "B = FOREACH A GENERATE name, TOTUPLE(url, picture) AS links;" +
                "DESCRIBE B;" +
                "ILLUSTRATE B;" +
                "STORE B INTO 'pig/tupleartists' USING org.elasticsearch.hadoop.pig.EsStorage();";
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
        assertThat(RestUtils.getMapping("pig/tupleartists").skipHeaders().toString(), is("tupleartists=[links=STRING, name=STRING]"));
    }

    @Test
    public void testBag() throws Exception {
        String script =
                "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                //"A = LOAD 'src/test/resources/artists.dat' USING PigStorage() AS (id:long, name, links:bag{t:(url:chararray, picture: chararray)});" +
                loadSource() +
                "B = FOREACH A GENERATE name, TOBAG(url, picture) AS links;" +
                "ILLUSTRATE B;" +
                "STORE B INTO 'pig/bagartists' USING org.elasticsearch.hadoop.pig.EsStorage();";
        pig.executeScript(script);
    }

    @Test
    public void testBagMapping() throws Exception {
        assertThat(RestUtils.getMapping("pig/bagartists").skipHeaders().toString(), is("bagartists=[links=STRING, name=STRING]"));
    }

    @Test
    public void testTimestamp() throws Exception {
        long millis = new Date().getTime();
        String script =
                "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                //"A = LOAD 'src/test/resources/artists.dat' USING PigStorage() AS (id:long, name, links:bag{t:(url:chararray, picture: chararray)});" +
                loadSource() +
                "B = FOREACH A GENERATE name, ToDate(" + millis + "l) AS date, url;" +
                "ILLUSTRATE B;" +
                "STORE B INTO 'pig/timestamp' USING org.elasticsearch.hadoop.pig.EsStorage();";

        pig.executeScript(script);
    }

    @Test
    public void testTimestampMapping() throws Exception {
        String mapping = RestUtils.getMapping("pig/timestamp").skipHeaders().toString();
        assertThat(mapping, containsString("date=DATE"));
    }

    @Test
    public void testFieldAlias() throws Exception {
        long millis = new Date().getTime();
        String script =
                "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                //"A = LOAD 'src/test/resources/artists.dat' USING PigStorage() AS (id:long, name, links:bag{t:(url:chararray, picture: chararray)});" +
                loadSource() +
                "B = FOREACH A GENERATE name, ToDate(" + millis + "l) AS timestamp, url, picture;" +
                "ILLUSTRATE B;" +
                "STORE B INTO 'pig/fieldalias' USING org.elasticsearch.hadoop.pig.EsStorage('es.mapping.names=nAme:@name, timestamp:@timestamp, uRL:url, picturE:picture');";

        pig.executeScript(script);
    }

    @Test
    public void testFieldAliasMapping() throws Exception {
        assertThat(RestUtils.getMapping("pig/fieldalias").skipHeaders().toString(), is("fieldalias=[@timestamp=DATE, name=STRING, picture=STRING, url=STRING]"));
    }

    @Test
    public void testCaseSensitivity() throws Exception {
        String script =
                "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                //"A = LOAD 'src/test/resources/artists.dat' USING PigStorage() AS (id:long, name, links:bag{t:(url:chararray, picture: chararray)});" +
                "A = LOAD '" + TestUtils.sampleArtistsDat() + "' USING PigStorage() AS (id:long, Name:chararray, uRL:chararray, pIctUre: chararray, timestamp: chararray); " +
                "B = FOREACH A GENERATE Name, uRL, pIctUre;" +
                "ILLUSTRATE B;" +
                "STORE B INTO 'pig/casesensitivity' USING org.elasticsearch.hadoop.pig.EsStorage();";

        pig.executeScript(script);
    }

    @Test
    public void testCaseSensitivityMapping() throws Exception {
        assertThat(RestUtils.getMapping("pig/casesensitivity").skipHeaders().toString(), is("casesensitivity=[Name=STRING, pIctUre=STRING, uRL=STRING]"));
    }

    @Test
    public void testEmptyComplexStructures() throws Exception {
        String script =
                "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                loadSource() +
                "AL = LIMIT A 10;" +
                "B = FOREACH AL GENERATE (), [], {};" +
                "STORE B INTO 'pig/emptyconst' USING org.elasticsearch.hadoop.pig.EsStorage();";

        pig.executeScript(script);
    }

    @Test
    public void testEmptyComplexStructuresMapping() throws Exception {
        assertThat(RestUtils.getMapping("pig/emptyconst").skipHeaders().toString(), is("emptyconst=[]"));
    }

    @Test
    public void testCreateWithId() throws Exception {
        String script =
                "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                loadSource() +
                "B = FOREACH A GENERATE id, name, TOBAG(url, picture) AS links;" +
                "STORE B INTO 'pig/createwithid' USING org.elasticsearch.hadoop.pig.EsStorage('"
                                + ConfigurationOptions.ES_WRITE_OPERATION + "=create','"
                                + ConfigurationOptions.ES_MAPPING_ID + "=id');";
        pig.executeScript(script);
    }

    @Test
    public void testCreateWithIdMapping() throws Exception {
        assertThat(RestUtils.getMapping("pig/createwithid").skipHeaders().toString(), is("createwithid=[id=LONG, links=STRING, name=STRING]"));
    }

    @Test(expected = EsHadoopIllegalStateException.class)
    public void testCreateWithIdShouldFailOnDuplicate() throws Exception {
        testCreateWithId();
    }

    @Test(expected = Exception.class)
    public void testUpdateWithoutId() throws Exception {
        String script =
                "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                loadSource() +
                "B = FOREACH A GENERATE id, name, TOBAG(url, picture) AS links;" +
                "STORE B INTO 'pig/updatewoid' USING org.elasticsearch.hadoop.pig.EsStorage('"
                                + ConfigurationOptions.ES_WRITE_OPERATION + "=update');";
        pig.executeScript(script);
    }

    @Test
    public void testUpdateWithId() throws Exception {
        String script =
                "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                loadSource() +
                "B = FOREACH A GENERATE id, name, TOBAG(url, picture) AS links;" +
                "STORE B INTO 'pig/update' USING org.elasticsearch.hadoop.pig.EsStorage('"
                                + ConfigurationOptions.ES_WRITE_OPERATION + "=upsert','"
                                + ConfigurationOptions.ES_MAPPING_ID + "=id');";
        pig.executeScript(script);
    }

    @Test
    public void testUpdateWithIdMapping() throws Exception {
        assertThat(RestUtils.getMapping("pig/update").skipHeaders().toString(), is("update=[id=LONG, links=STRING, name=STRING]"));
    }

    @Test(expected = EsHadoopIllegalStateException.class)
    public void testUpdateWithoutUpsert() throws Exception {
        String script =
                "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                loadSource() +
                "B = FOREACH A GENERATE id, name, TOBAG(url, picture) AS links;" +
                "STORE B INTO 'pig/updatewoupsert' USING org.elasticsearch.hadoop.pig.EsStorage('"
                                + ConfigurationOptions.ES_WRITE_OPERATION + "=update','"
                                + ConfigurationOptions.ES_MAPPING_ID + "=id');";
        pig.executeScript(script);
    }

    @Test
    public void testParentChild() throws Exception {
        RestUtils.putMapping("pig/child", "org/elasticsearch/hadoop/integration/mr-child.json");

        String script =
                "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                loadSource() +
                "B = FOREACH A GENERATE id, name, TOBAG(url, picture) AS links;" +
                "STORE B INTO 'pig/child' USING org.elasticsearch.hadoop.pig.EsStorage('"
                                + ConfigurationOptions.ES_MAPPING_PARENT + "=id','"
                                + ConfigurationOptions.ES_INDEX_AUTO_CREATE + "=no');";
        pig.executeScript(script);
    }

    @Test
    public void testParentChildMapping() throws Exception {
        assertThat(RestUtils.getMapping("pig/child").skipHeaders().toString(), is("child=[id=LONG, links=STRING, name=STRING]"));
    }

    @Test
    public void testNestedTuple() throws Exception {
        RestUtils.postData("pig/nestedtuple", "{\"my_array\" : [\"1.a\",\"1.b\"]}".getBytes(StringUtils.UTF_8));
        RestUtils.postData("pig/nestedtuple", "{\"my_array\" : [\"2.a\",\"2.b\"]}".getBytes(StringUtils.UTF_8));
        RestUtils.waitForYellow("pig");
    }


    @Test
    public void testIndexPattern() throws Exception {
        String script =
                "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                loadSource() +
                "STORE A INTO 'pig/pattern-{id}' USING org.elasticsearch.hadoop.pig.EsStorage();";

        pig.executeScript(script);
    }

    @Test
    public void testIndexPatternMapping() throws Exception {
        assertThat(RestUtils.getMapping("pig/pattern-123").skipHeaders().toString(), is("pattern-123=[id=LONG, name=STRING, picture=STRING, timestamp=DATE, url=STRING]"));
    }

    @Test
    public void testIndexPatternFormat() throws Exception {
        String script =
                "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                loadSource() +
                "STORE A INTO 'pig/pattern-format-{timestamp:YYYY-MM-dd}' USING org.elasticsearch.hadoop.pig.EsStorage();";

        pig.executeScript(script);
    }

    @Test
    public void testIndexPatternFormatMapping() throws Exception {
        assertThat(RestUtils.getMapping("pig/pattern-format-2001-10-06").skipHeaders().toString(), is("pattern-format-2001-10-06=[id=LONG, name=STRING, picture=STRING, timestamp=DATE, url=STRING]"));
    }

    private String loadSource() {
        return "A = LOAD '" + TestUtils.sampleArtistsDat() + "' USING PigStorage() AS (id:long, name:chararray, url:chararray, picture: chararray, timestamp: chararray);";
    }
}