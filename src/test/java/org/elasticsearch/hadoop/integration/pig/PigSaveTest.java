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
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.integration.Provisioner;
import org.elasticsearch.hadoop.rest.RestClient;
import org.elasticsearch.hadoop.util.RestUtils;
import org.elasticsearch.hadoop.util.StringUtils;
import org.elasticsearch.hadoop.util.TestSettings;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class PigSaveTest {

    static PigWrapper pig;

    @BeforeClass
    public static void startup() throws Exception {
        pig = new PigWrapper();
        pig.start();

        // initialize Pig in local mode
        RestClient client = new RestClient(new TestSettings());
        try {
            client.deleteIndex("pig");
        } catch (Exception ex) {
            // ignore
        }
    }

    @AfterClass
    public static void shutdown() {
        pig.stop();
    }

    @Test
    public void testTuple() throws Exception {
        String script =
                "SET mapred.map.tasks 2;" +
                "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                // "A = LOAD 'src/test/resources/artists.dat' USING PigStorage() AS (id:long, name:chararray, links:tuple(url:chararray, picture: chararray));" +
                "A = LOAD 'src/test/resources/artists.dat' USING PigStorage() AS (id:long, name:chararray, url:chararray, picture: chararray);" +
                //"ILLUSTRATE A;" +
                "B = FOREACH A GENERATE name, TOTUPLE(url, picture) AS links;" +
                //"ILLUSTRATE B;" +
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
    public void testBag() throws Exception {
        String script =
                "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                //"A = LOAD 'src/test/resources/artists.dat' USING PigStorage() AS (id:long, name, links:bag{t:(url:chararray, picture: chararray)});" +
                "A = LOAD 'src/test/resources/artists.dat' USING PigStorage() AS (id:long, name:chararray, url:chararray, picture: chararray);" +
                "B = FOREACH A GENERATE name, TOBAG(url, picture) AS links;" +
                "ILLUSTRATE B;" +
                "STORE B INTO 'pig/bagartists' USING org.elasticsearch.hadoop.pig.EsStorage();";
        pig.executeScript(script);
    }

    @Test
    public void testTimestamp() throws Exception {
        long millis = new Date().getTime();
        String script =
                "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                //"A = LOAD 'src/test/resources/artists.dat' USING PigStorage() AS (id:long, name, links:bag{t:(url:chararray, picture: chararray)});" +
                "A = LOAD 'src/test/resources/artists.dat' USING PigStorage() AS (id:long, name:chararray, url:chararray, picture: chararray);" +
                "B = FOREACH A GENERATE name, ToDate(" + millis + "l), url;" +
                "ILLUSTRATE B;" +
                "STORE B INTO 'pig/timestamp' USING org.elasticsearch.hadoop.pig.EsStorage();";

        pig.executeScript(script);
    }

    @Test
    public void testFieldAlias() throws Exception {
        long millis = new Date().getTime();
        String script =
                "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                //"A = LOAD 'src/test/resources/artists.dat' USING PigStorage() AS (id:long, name, links:bag{t:(url:chararray, picture: chararray)});" +
                "A = LOAD 'src/test/resources/artists.dat' USING PigStorage() AS (Id:long, name:chararray, url:chararray, picture: chararray);" +
                "B = FOREACH A GENERATE name, ToDate(" + millis + "l) AS timestamp, url, picture;" +
                "ILLUSTRATE B;" +
                "STORE B INTO 'pig/fieldalias' USING org.elasticsearch.hadoop.pig.EsStorage('es.mapping.names=nAme:@name, timestamp:@timestamp, uRL:url, picturE:picture');";

        pig.executeScript(script);
    }

    @Test
    public void testEmptyComplexStructures() throws Exception {
        String script =
                "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                "A = LOAD 'src/test/resources/artists.dat' USING PigStorage() AS (id:long, name:chararray, url:chararray, picture: chararray);" +
                "AL = LIMIT A 10;" +
                "B = FOREACH AL GENERATE (), [], {};" +
                "STORE B INTO 'pig/emptyconst' USING org.elasticsearch.hadoop.pig.EsStorage();";

        pig.executeScript(script);
    }

    @Test
    public void testCreateWithId() throws Exception {
        String script =
                "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                "A = LOAD 'src/test/resources/artists.dat' USING PigStorage() AS (id:long, name:chararray, url:chararray, picture: chararray);" +
                "B = FOREACH A GENERATE id, name, TOBAG(url, picture) AS links;" +
                "STORE B INTO 'pig/createwithid' USING org.elasticsearch.hadoop.pig.EsStorage('"
                                + ConfigurationOptions.ES_WRITE_OPERATION + "=create','"
                                + ConfigurationOptions.ES_MAPPING_ID + "=id');";
        pig.executeScript(script);
    }

    @Test(expected = EsHadoopIllegalStateException.class)
    public void testCreateWithIdShouldFailOnDuplicate() throws Exception {
        testCreateWithId();
    }

    @Test(expected = Exception.class)
    public void testUpdateWithoutId() throws Exception {
        String script =
                "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                "A = LOAD 'src/test/resources/artists.dat' USING PigStorage() AS (id:long, name:chararray, url:chararray, picture: chararray);" +
                "B = FOREACH A GENERATE id, name, TOBAG(url, picture) AS links;" +
                "STORE B INTO 'pig/updatewoid' USING org.elasticsearch.hadoop.pig.EsStorage('"
                                + ConfigurationOptions.ES_WRITE_OPERATION + "=update');";
        pig.executeScript(script);
    }

    @Test
    public void testUpdateWithId() throws Exception {
        String script =
                "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                "A = LOAD 'src/test/resources/artists.dat' USING PigStorage() AS (id:long, name:chararray, url:chararray, picture: chararray);" +
                "B = FOREACH A GENERATE id, name, TOBAG(url, picture) AS links;" +
                "STORE B INTO 'pig/update' USING org.elasticsearch.hadoop.pig.EsStorage('"
                                + ConfigurationOptions.ES_WRITE_OPERATION + "=update','"
                                + ConfigurationOptions.ES_MAPPING_ID + "=id');";
        pig.executeScript(script);
    }

    @Test(expected = EsHadoopIllegalStateException.class)
    public void testUpdateWithoutUpsert() throws Exception {
        String script =
                "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                "A = LOAD 'src/test/resources/artists.dat' USING PigStorage() AS (id:long, name:chararray, url:chararray, picture: chararray);" +
                "B = FOREACH A GENERATE id, name, TOBAG(url, picture) AS links;" +
                "STORE B INTO 'pig/updatewoupsert' USING org.elasticsearch.hadoop.pig.EsStorage('"
                                + ConfigurationOptions.ES_WRITE_OPERATION + "=update','"
                                + ConfigurationOptions.ES_MAPPING_ID + "=id','"
                                + ConfigurationOptions.ES_UPSERT_DOC + "=false');";
        pig.executeScript(script);
    }

    @Test
    public void testParentChild() throws Exception {
        RestUtils.putMapping("pig/child", "org/elasticsearch/hadoop/integration/mr-child.json");

        String script =
                "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                "A = LOAD 'src/test/resources/artists.dat' USING PigStorage() AS (id:long, name:chararray, url:chararray, picture: chararray);" +
                "B = FOREACH A GENERATE id, name, TOBAG(url, picture) AS links;" +
                "STORE B INTO 'pig/child' USING org.elasticsearch.hadoop.pig.EsStorage('"
                                + ConfigurationOptions.ES_MAPPING_PARENT + "=id','"
                                + ConfigurationOptions.ES_INDEX_AUTO_CREATE + "=no');";
        pig.executeScript(script);
    }

    @Test
    public void testNestedTuple() throws Exception {
        RestUtils.putData("pig/nestedtuple", "{\"my_array\" : [\"1.a\",\"1.b\"]}".getBytes(StringUtils.UTF_8));
        RestUtils.putData("pig/nestedtuple", "{\"my_array\" : [\"2.a\",\"2.b\"]}".getBytes(StringUtils.UTF_8));
        RestUtils.waitForYellow("pig");
    }
}