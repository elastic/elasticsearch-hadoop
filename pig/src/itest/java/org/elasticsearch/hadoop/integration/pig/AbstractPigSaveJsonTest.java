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

import org.elasticsearch.hadoop.EsHadoopIllegalStateException;
import org.elasticsearch.hadoop.Provisioner;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.mr.RestUtils;
import org.elasticsearch.hadoop.rest.RestClient;
import org.elasticsearch.hadoop.util.TestSettings;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;


@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class AbstractPigSaveJsonTest extends AbstractPigTests {

    @BeforeClass
    public static void startup() throws Exception {
        AbstractPigTests.startup();
        // initialize Pig in local mode
        RestClient client = new RestClient(new TestSettings());
        try {
            client.delete("json-pig");
        } catch (Exception ex) {
            // ignore
        }
    }

    @Test
    public void testTuple() throws Exception {
        String script =
                "SET mapred.map.tasks 2;" +
                "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                loadSource() +
                //"ILLUSTRATE A;" +
                "STORE A INTO 'json-pig/tupleartists' USING org.elasticsearch.hadoop.pig.EsStorage('es.input.json=true');";
        //"es_total = LOAD 'radio/artists/_count?q=me*' USING org.elasticsearch.hadoop.pig.EsStorage();" +
        pig.executeScript(script);
    }

    @Test
    public void testFieldAlias() throws Exception {
        String script =
                "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                loadSource() +
                "STORE A INTO 'json-pig/fieldalias' USING org.elasticsearch.hadoop.pig.EsStorage('es.input.json=true','es.mapping.names=data:@json');";

        pig.executeScript(script);
    }

    @Test
    public void testCreateWithId() throws Exception {
        String script =
                "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                loadSource() +
                "STORE A INTO 'json-pig/createwithid' USING org.elasticsearch.hadoop.pig.EsStorage('"
                                + ConfigurationOptions.ES_WRITE_OPERATION + "=create','"
                                + ConfigurationOptions.ES_MAPPING_ID + "=number',"
                                + "'es.input.json=true');";
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
                loadSource() +
                "STORE A INTO 'json-pig/updatewoid' USING org.elasticsearch.hadoop.pig.EsStorage('"
                                + ConfigurationOptions.ES_WRITE_OPERATION + "=update',"
                                + "'es.input.json=true');";
        pig.executeScript(script);
    }

    @Test
    public void testUpdateWithId() throws Exception {
        String script =
                "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                loadSource() +
                "STORE A INTO 'json-pig/update' USING org.elasticsearch.hadoop.pig.EsStorage('"
                                + ConfigurationOptions.ES_WRITE_OPERATION + "=upsert','"
                                + ConfigurationOptions.ES_MAPPING_ID + "=number',"
                                + "'es.input.json=true');";
        pig.executeScript(script);
    }

    @Test(expected = EsHadoopIllegalStateException.class)
    public void testUpdateWithoutUpsert() throws Exception {
        String script =
                "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                loadSource() +
                "STORE A INTO 'json-pig/updatewoupsert' USING org.elasticsearch.hadoop.pig.EsStorage('"
                                + ConfigurationOptions.ES_WRITE_OPERATION + "=update','"
                                + ConfigurationOptions.ES_MAPPING_ID + "=number',"
                                + "'es.input.json=true');";
        pig.executeScript(script);
    }

    @Test
    public void testParentChild() throws Exception {
        RestUtils.putMapping("json-pig/child", "org/elasticsearch/hadoop/integration/mr-child.json");

        String script =
                "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                loadSource() +
                "STORE A INTO 'json-pig/child' USING org.elasticsearch.hadoop.pig.EsStorage('"
                                + ConfigurationOptions.ES_MAPPING_PARENT + "=number','"
                                + ConfigurationOptions.ES_INDEX_AUTO_CREATE + "=no',"
                                + "'es.input.json=true');";
        pig.executeScript(script);
    }

    @Test
    public void testIndexPattern() throws Exception {
        String script =
                "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                loadSource() +
                "STORE A INTO 'json-pig/pattern-{number}' USING org.elasticsearch.hadoop.pig.EsStorage('es.input.json=true');";

        pig.executeScript(script);
    }

    @Test
    public void testIndexPatternFormat() throws Exception {
        String script =
                "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                loadSource() +
                "STORE A INTO 'json-pig/pattern-format-{@timestamp:YYYY-MM-dd}' USING org.elasticsearch.hadoop.pig.EsStorage('es.input.json=true');";

        pig.executeScript(script);
    }

    private String loadSource() {
        return "A = LOAD '" + org.elasticsearch.hadoop.util.TestUtils.sampleArtistsJson() + "' USING PigStorage() AS (json: chararray);";
    }
}