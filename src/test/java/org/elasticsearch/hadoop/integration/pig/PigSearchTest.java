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
package org.elasticsearch.hadoop.integration.pig;

import java.util.Collection;

import org.elasticsearch.hadoop.integration.Provisioner;
import org.elasticsearch.hadoop.integration.QueryTestParams;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class PigSearchTest {

    static PigWrapper pig;

    @Parameters
    public static Collection<Object[]> queries() {
        return QueryTestParams.params();
    }

    private String query;

    public PigSearchTest(String query) {
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
    public void testTuple() throws Exception {
        String script =
                "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                "DEFINE ESStorage org.elasticsearch.hadoop.pig.ESStorage('es.query=" + query + "');" +
                "A = LOAD 'pig/tupleartists' USING ESStorage();";
                //"DESCRIBE A;";
                //"//DUMP A;";
        pig.executeScript(script);
    }

    @Test
    public void testBag() throws Exception {
        String script =
                      "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                      "DEFINE ESStorage org.elasticsearch.hadoop.pig.ESStorage('es.query=" + query + "');"
                      + "A = LOAD 'pig/bagartists' USING ESStorage();"
                      + "DESCRIBE A;"
                      + "DUMP A;";
        pig.executeScript(script);
    }

    @Test
    public void testTimestamp() throws Exception {
        String script =
                      "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                      "DEFINE ESStorage org.elasticsearch.hadoop.pig.ESStorage('es.query=" + query + "');"
                      + "A = LOAD 'pig/timestamp' USING ESStorage();"
                      + "DESCRIBE A;"
                      + "DUMP A;";
        pig.executeScript(script);
    }

    @Test
    public void testFieldAlias() throws Exception {
        String script =
                      "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                      "DEFINE ESStorage org.elasticsearch.hadoop.pig.ESStorage('es.mapping.names=nAme:name, timestamp:@timestamp, uRL:url, picturE:picture', 'es.query=" + query + "');"
                      + "A = LOAD 'pig/fieldalias' USING ESStorage();"
                      + "DESCRIBE A;"
                      + "DUMP A;";
        pig.executeScript(script);
    }

    @Test
    public void testMissingIndex() throws Exception {
        String script =
                      "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                      "DEFINE ESStorage org.elasticsearch.hadoop.pig.ESStorage('es.index.read.missing.as.empty=true','es.query=" + query + "');"
                      + "A = LOAD 'foo/bar' USING ESStorage();"
                      + "DESCRIBE A;"
                      + "DUMP A;";
        pig.executeScript(script);
    }

    @Test
    public void testParentChild() throws Exception {
        String script =
                      "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                      "DEFINE ESStorage org.elasticsearch.hadoop.pig.ESStorage('es.index.read.missing.as.empty=true','es.query=" + query + "');"
                      + "A = LOAD 'pig/child' USING ESStorage();"
                      + "DESCRIBE A;"
                      + "DUMP A;";
        pig.executeScript(script);
    }

}