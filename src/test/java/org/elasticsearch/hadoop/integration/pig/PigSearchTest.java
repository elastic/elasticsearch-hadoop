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

import org.elasticsearch.hadoop.integration.Provisioner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 */
public class PigSearchTest {

    static LocalPig pig;

    @BeforeClass
    public static void startup() throws Exception {
        pig = new LocalPig();
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
                "DEFINE ESStorage org.elasticsearch.hadoop.pig.ESStorage();" +
                "A = LOAD 'pig/tupleartists/_search?q=me*' USING ESStorage();";
                //"DESCRIBE A;";
                //"//DUMP A;";
        pig.executeScript(script);
    }

    @Test
    public void testBag() throws Exception {
        String script =
                      "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                      "DEFINE ESStorage org.elasticsearch.hadoop.pig.ESStorage();"
                      + "A = LOAD 'pig/bagartists/_search?q=me*' USING ESStorage();"
                      + "DESCRIBE A;"
                      + "DUMP A;";
        pig.executeScript(script);
    }

    @Test
    public void testTimestamp() throws Exception {
        String script =
                      "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                      "DEFINE ESStorage org.elasticsearch.hadoop.pig.ESStorage();"
                      + "A = LOAD 'pig/timestamp/_search?q=me*' USING ESStorage();"
                      + "DESCRIBE A;"
                      + "DUMP A;";
        pig.executeScript(script);
    }

    @Test
    public void testFieldAlias() throws Exception {
        String script =
                      "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                      "DEFINE ESStorage org.elasticsearch.hadoop.pig.ESStorage();"
                      + "A = LOAD 'pig/fieldalias/_search?q=me*' USING ESStorage('es.column.aliases=nAme:name, timestamp:@timestamp, uRL:url, picturE:picture');"
                      + "DESCRIBE A;"
                      + "DUMP A;";
        pig.executeScript(script);
    }
}
