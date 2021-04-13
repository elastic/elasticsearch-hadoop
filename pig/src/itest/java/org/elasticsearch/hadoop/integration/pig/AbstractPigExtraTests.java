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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.elasticsearch.hadoop.HdpBootstrap;
import org.elasticsearch.hadoop.TestData;
import org.elasticsearch.hadoop.fs.HdfsUtils;
import org.elasticsearch.hadoop.mr.HadoopCfgUtils;
import org.elasticsearch.hadoop.rest.RestUtils;
import org.elasticsearch.hadoop.util.EsMajorVersion;
import org.elasticsearch.hadoop.util.TestUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

import static org.elasticsearch.hadoop.util.TestUtils.docEndpoint;
import static org.elasticsearch.hadoop.util.TestUtils.resource;
import static org.junit.Assert.*;

import static org.hamcrest.CoreMatchers.*;


public class AbstractPigExtraTests extends AbstractPigTests {

    private EsMajorVersion VERSION = TestUtils.getEsClusterInfo().getMajorVersion();
    private static String PIG_DATA_DIR = "/pig-data/";
    private static Configuration testConfiguration = HdpBootstrap.hadoopConfig();
    private static String workingDir = HadoopCfgUtils.isLocal(testConfiguration) ? Paths.get("").toAbsolutePath().toString() : "/";

    @ClassRule
    public static TestData testData = new TestData();

    @BeforeClass
    public static void beforeClass() throws Exception {
        if (HadoopCfgUtils.isLocal(testConfiguration) == false) {
            File parentTxt = testData.unpackResource("/parent.txt");
            File childTxt = testData.unpackResource("/child.txt");
            File groupSampleTxt = testData.unpackResource("/group-sample.txt");
            File tupleTxt = testData.unpackResource("/tuple.txt");

            HdfsUtils.copyFromLocal(parentTxt.toURI().toString(), PIG_DATA_DIR + "parent.txt");
            HdfsUtils.copyFromLocal(childTxt.toURI().toString(), PIG_DATA_DIR + "child.txt");
            HdfsUtils.copyFromLocal(groupSampleTxt.toURI().toString(), PIG_DATA_DIR + "group-sample.txt");
            HdfsUtils.copyFromLocal(tupleTxt.toURI().toString(), PIG_DATA_DIR + "tuple.txt");
        }
    }

    private static String resourceFile(String filename) throws IOException {
        if (HadoopCfgUtils.isLocal(testConfiguration)) {
            return testData.unpackResource(filename).toURI().toString();
        } else {
            return PIG_DATA_DIR + filename;
        }
    }

    @Test
    public void testJoin() throws Exception {
        String script =
                "PARENT = LOAD '" + resourceFile("/parent.txt") + "' using PigStorage('|') as (parent_name: chararray, parent_value: chararray);" +
                "CHILD = LOAD '" + resourceFile("/child.txt") + "' using PigStorage('|') as (child_name: chararray, parent_name: chararray, child_value: long);" +
                "STORE PARENT into '"+ resource("pig-test-parent", "data", VERSION)+"' using org.elasticsearch.hadoop.pig.EsStorage();" +
                "STORE CHILD into '"+resource("pig-test-child", "data", VERSION)+"' using org.elasticsearch.hadoop.pig.EsStorage();";
       String script2 =
                "ES_PARENT = LOAD '"+resource("pig-test-parent", "data", VERSION)+"' using org.elasticsearch.hadoop.pig.EsStorage() as (parent_name: chararray, parent_value: chararray);" +
                "ES_CHILD = LOAD '"+resource("pig-test-child", "data", VERSION)+"' using org.elasticsearch.hadoop.pig.EsStorage() as (child_name: chararray, parent_name: chararray, child_value: long);" +
                "CO_GROUP = COGROUP ES_PARENT by parent_name, ES_CHILD by parent_name;" +
                "PARENT_CHILD = JOIN ES_PARENT by parent_name, ES_CHILD by parent_name;" +
                "STORE PARENT_CHILD INTO '" + tmpPig() + "/testjoin-join';" +
                "STORE CO_GROUP INTO '" + tmpPig() + "/testjoin-cogroup';";
        pig.executeScript(script);
        pig.executeScript(script2);

        String join = getResults("" + tmpPig() + "/testjoin-join");
        assertThat(join, containsString(tabify("parent1", "name1", "child1", "parent1", "100")));
        assertThat(join, containsString(tabify("parent1", "name1", "child2", "parent1", "200")));
        assertThat(join, containsString(tabify("parent2", "name2", "child3", "parent2", "300")));

        String cogroup = getResults("" + tmpPig() + "/testjoin-cogroup");
        assertThat(cogroup, containsString(tabify("parent1", "{(parent1,name1)}")));
        // bags are not ordered so check each tuple individually
        assertThat(cogroup, containsString("(child2,parent1,200)"));
        assertThat(cogroup, containsString("(child1,parent1,100)"));
        assertThat(cogroup, containsString(tabify("parent2", "{(parent2,name2)}", "{(child3,parent2,300)}")));
    }

    @Test
    public void testTemporarySchema() throws Exception {
        RestUtils.touch("pig-test-temp_schema");

        String script =
                "data = LOAD '" + resourceFile("/group-sample.txt") + "' using PigStorage(',') as (no:long,name:chararray,age:long);" +
                "data_limit = LIMIT data 1;" +
                "data_final = FOREACH data_limit GENERATE TRIM(name) as details, no as number;" +
                "STORE data_final into '"+resource("pig-test-temp_schema", "data", VERSION)+"' using org.elasticsearch.hadoop.pig.EsStorage('es.mapping.id=details');";
        pig.executeScript(script);
    }

    @Test
    public void testIterate() throws Exception {
        RestUtils.touch("pig-test-iterate");
        RestUtils.postData(docEndpoint("pig-test-iterate", "data", VERSION), "{\"message\" : \"Hello World\",\"message_date\" : \"2014-05-25\"}".getBytes());
        RestUtils.postData(docEndpoint("pig-test-iterate", "data", VERSION), "{\"message\" : \"Goodbye World\",\"message_date\" : \"2014-05-25\"}".getBytes());
        RestUtils.refresh("pig-test-iterate");

        String script =
                "data = LOAD '"+resource("pig-test-iterate", "data", VERSION)+"' using org.elasticsearch.hadoop.pig.EsStorage() as (message:chararray,message_date:chararray);" +
                "data = FOREACH data GENERATE message_date as date, message as message;" +
                "STORE data INTO '" + tmpPig() + "/pig-iterate';";
        pig.executeScript(script);

        String iterate = getResults("" + tmpPig() + "/pig-iterate");
        assertThat(iterate, containsString("World"));
    }
    

    @Test
    public void testTupleSaving() throws Exception {

        String script =
                "answers = LOAD '" + resourceFile("/tuple.txt") + "' using PigStorage(',') as (id:int, parentId:int, score:int);" +
                "grouped = GROUP answers by id;" +
                "ILLUSTRATE grouped;" +
                "STORE grouped into '"+resource("pig-test-tuple-structure", "data", VERSION)+"' using org.elasticsearch.hadoop.pig.EsStorage('es.mapping.pig.tuple.use.field.names = true');";
        pig.executeScript(script);

        String string = RestUtils.get("pig-test-tuple-structure/_search");
        assertThat(string, containsString("parentId"));
    }

    private static String tmpPig() {
        return new Path("tmp-pig/extra")
                .makeQualified(FileSystem.getDefaultUri(AbstractPigTests.testConfiguration), new Path(workingDir))
                .toUri()
                .toString();
    }
}
