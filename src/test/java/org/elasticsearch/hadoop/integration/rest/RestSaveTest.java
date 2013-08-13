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
package org.elasticsearch.hadoop.integration.rest;

import java.nio.charset.Charset;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Scanner;

import org.apache.derby.impl.sql.catalog.SYSSCHEMASRowFactory;
import org.apache.derby.impl.store.access.UTF;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.rest.BufferedRestClient;
import org.elasticsearch.hadoop.serialization.JdkValueWriter;
import org.elasticsearch.hadoop.util.BytesArray;
import org.elasticsearch.hadoop.util.StringUtils;
import org.elasticsearch.hadoop.util.TestSettings;
import org.junit.Test;

public class RestSaveTest {

    @Test
    public void testBulkWrite() throws Exception {
        TestSettings testSettings = new TestSettings("rest/savebulk");
        //testSettings.setPort(9200)
        testSettings.setProperty(ConfigurationOptions.ES_SERIALIZATION_WRITER_CLASS, JdkValueWriter.class.getName());
        BufferedRestClient client = new BufferedRestClient(testSettings);

        Scanner in = new Scanner(getClass().getResourceAsStream("/artists.dat")).useDelimiter("\\n|\\t");

        Map<String, String> line = new LinkedHashMap<String, String>();

        for (; in.hasNextLine();) {
            // ignore number
            in.next();
            line.put("name", in.next());
            line.put("url", in.next());
            line.put("picture", in.next());
            client.addToIndex(line);
            line.clear();
        }

        client.close();
    }

//    @Test
    public void testBulkUpsert() throws Exception {

        byte[] SCHEMA = ("{\n" +
                "        \"savebulk\" : {\n" +
                "                \"_id\" : {\n \"path\" : \"url\"\n}\n" +
                "        }\n" +
                "}"
        ).getBytes(StringUtils.UTF_8);

        TestSettings testSettings = new TestSettings("rest555/savebulk");
        testSettings.setProperty(ConfigurationOptions.ES_SERIALIZATION_WRITER_CLASS, JdkValueWriter.class.getName());
        testSettings.setProperty(ConfigurationOptions.ES_INDEX_WRITE_STRATEGY, ConfigurationOptions.ES_INDEX_WRITE_STRATEGY_UPSERT);
        testSettings.setProperty(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "no");
        testSettings.setProperty(ConfigurationOptions.ES_HOST, "davesdream.conductor.com");
        testSettings.setProperty(ConfigurationOptions.ES_PORT, "9200");

        BufferedRestClient client = new BufferedRestClient(testSettings);
        client.putMapping(new BytesArray(SCHEMA, SCHEMA.length));

        Scanner in = new Scanner(getClass().getResourceAsStream("/artists.dat")).useDelimiter("\\n|\\t");

        Map<String, String> line = new LinkedHashMap<String, String>();

        for (; in.hasNextLine();) {
            // ignore number
            in.next();
            line.put("name", in.next());
            line.put("url", in.next());
            line.put("picture", in.next());
            client.addToIndex(line);
            line.clear();
        }

        client.close();
    }
}
