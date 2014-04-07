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

import java.util.Collection;

import org.elasticsearch.hadoop.Provisioner;
import org.elasticsearch.hadoop.QueryTestParams;
import org.elasticsearch.hadoop.mr.RestUtils;
import org.elasticsearch.hadoop.util.StringUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class AbstractPigSearchJsonTest {

    static PigWrapper pig;

    @Parameters
    public static Collection<Object[]> queries() {
        return QueryTestParams.params();
    }

    private final String query;

    public AbstractPigSearchJsonTest(String query) {
        this.query = query;
    }


    @BeforeClass
    public static void startup() throws Exception {
        pig = new PigWrapper();
        pig.start();
    }

    @AfterClass
    public static void shutdown() {
        pig.stop();
    }


    @Test
    public void testNestedField() throws Exception {
        String data = "{ \"data\" : { \"map\" : { \"key\" :  10  } } }";
        RestUtils.putData("json-pig/nestedmap", StringUtils.toUTF(data));

        //RestUtils.waitForYellow("json-hive");
        RestUtils.refresh("json-pig");

        String script =
                "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                "DEFINE EsStorage org.elasticsearch.hadoop.pig.EsStorage('es.mapping.names=nested:data.map.key');" +
                //"A = LOAD 'json-pig/nestedmap' USING EsStorage() AS (nested:tuple(key:int));" +
                "A = LOAD 'json-pig/nestedmap' USING EsStorage() AS (nested:chararray);" +
                "DESCRIBE A;" +
                "X = LIMIT A 3;" +
                "DUMP X;";
        pig.executeScript(script);

//        script =
//                "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
//                "DEFINE EsStorage org.elasticsearch.hadoop.pig.EsStorage('es.query=" + query + "','es.mapping.names=nested:data.map');" +
//                "A = LOAD 'json-pig/nestedmap' USING EsStorage() AS (key:chararray, value:);" +
//                "DESCRIBE A;" +
//                "X = LIMIT A 3;" +
//                "DUMP X;";
//        pig.executeScript(script);
    }



    @Test
    public void testTuple() throws Exception {
        String script =
                "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                "DEFINE EsStorage org.elasticsearch.hadoop.pig.EsStorage('es.query=" + query + "');" +
                "A = LOAD 'json-pig/tupleartists' USING EsStorage();" +
                "X = LIMIT A 3;" +
                //"DESCRIBE A;";
                "DUMP X;";
        pig.executeScript(script);
    }

    @Test
    public void testTupleWithSchema() throws Exception {
        String script =
                "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                "DEFINE EsStorage org.elasticsearch.hadoop.pig.EsStorage('es.query=" + query + "');" +
                "A = LOAD 'json-pig/tupleartists' USING EsStorage() AS (name:chararray);" +
                //"DESCRIBE A;" +
                "X = LIMIT A 3;" +
                "DUMP X;";
        pig.executeScript(script);
    }

    @Test
    public void testFieldAlias() throws Exception {
        String script =
                      "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                       "DEFINE EsStorage org.elasticsearch.hadoop.pig.EsStorage('es.query="+ query + "');"
                      + "A = LOAD 'json-pig/fieldalias' USING EsStorage();"
                      + "X = LIMIT A 3;"
                      + "DUMP X;";
        pig.executeScript(script);
    }

    @Test
    public void testMissingIndex() throws Exception {
        String script =
                      "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                      "DEFINE EsStorage org.elasticsearch.hadoop.pig.EsStorage('es.index.read.missing.as.empty=true','es.query=" + query + "');"
                      + "A = LOAD 'foo/bar' USING EsStorage();"
                      + "X = LIMIT A 3;"
                      + "DUMP X;";
        pig.executeScript(script);
    }

    @Test
    public void testParentChild() throws Exception {
        String script =
                      "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                      "DEFINE EsStorage org.elasticsearch.hadoop.pig.EsStorage('es.index.read.missing.as.empty=true','es.query=" + query + "');"
                      + "A = LOAD 'json-pig/child' USING EsStorage();"
                      + "X = LIMIT A 3;"
                      + "DUMP X;";
        pig.executeScript(script);
    }

    @Test
    public void testDynamicPattern() throws Exception {
        Assert.assertTrue(RestUtils.exists("json-pig/pattern-1"));
        Assert.assertTrue(RestUtils.exists("json-pig/pattern-500"));
        Assert.assertTrue(RestUtils.exists("json-pig/pattern-990"));
    }
}