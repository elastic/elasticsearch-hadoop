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

import java.io.ByteArrayInputStream;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.elasticsearch.hadoop.integration.TestSettings;
import org.elasticsearch.hadoop.rest.RestClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 */
public class PigSaveTest {

    static PigServer pig;

    @BeforeClass
    public static void startup() throws Exception {
        // initialize Pig in local mode
        pig = new PigServer(ExecType.LOCAL, TestSettings.TESTING_PROPS);
        pig.setBatchOn();
        RestClient client = new RestClient(new TestSettings());
        try {
            client.deleteIndex("radio");
        } catch (Exception ex) {
            // ignore
        }
    }

    @AfterClass
    public static void shutdown() {
        // close pig
        if (pig != null) {
            pig.shutdown();
            pig = null;
        }
    }

    @Test
    public void testStorageBasic() throws Exception {
        String script =
                // "A = LOAD 'src/test/resources/artists.dat' USING PigStorage() AS (id:long, name:chararray, links:tuple(url:chararray, picture: chararray));" +
                "A = LOAD 'src/test/resources/artists.dat' USING PigStorage() AS (id:long, name, url:chararray, picture: chararray);" +
                //"ILLUSTRATE A;" +
                "B = FOREACH A GENERATE name, TOTUPLE(url, picture) AS links;" +
                //"ILLUSTRATE B;" +
                "STORE B INTO 'pig/artists' USING org.elasticsearch.hadoop.pig.ESStorage();";
                //"es_total = LOAD 'radio/artists/_count?q=me*' USING org.elasticsearch.hadoop.pig.ESStorage();" +
                //"DUMP es_total;" +
                //"bartists = FILTER B BY name MATCHES 'me.*';" +
                //"allb = GROUP bartists ALL;"+
                //"total = FOREACH allb GENERATE 'total' as foo, COUNT_STAR($1) as total;"+
                //"ILLUSTRATE allb;"+
                //"STORE total INTO '/tmp/total';"+
                //"DUMP total;";
        executeScript(script);
    }

    private void executeScript(String script) throws Exception {
        pig.registerScript(new ByteArrayInputStream(script.getBytes()));
        pig.executeBatch();
        pig.discardBatch();
    }
}
