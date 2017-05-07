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

import org.elasticsearch.hadoop.Provisioner;
import org.elasticsearch.hadoop.mr.RestUtils;
import org.junit.Test;

import static org.junit.Assert.*;

import static org.hamcrest.CoreMatchers.*;


public class AbstractPigExtraTests extends AbstractPigTests {

    @Test
    public void testJoin() throws Exception {
        String script =
                "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                "PARENT = LOAD 'src/itest/resources/parent.txt' using PigStorage('|') as (parent_name: chararray, parent_value: chararray);" +
                "CHILD = LOAD 'src/itest/resources/child.txt' using PigStorage('|') as (child_name: chararray, parent_name: chararray, child_value: long);" +
                "STORE PARENT into 'pig-test-parent/data' using org.elasticsearch.hadoop.pig.EsStorage();" +
                "STORE CHILD into 'pig-test-child/data' using org.elasticsearch.hadoop.pig.EsStorage();";
       String script2 =
                "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                "ES_PARENT = LOAD 'pig-test-parent/data' using org.elasticsearch.hadoop.pig.EsStorage() as (parent_name: chararray, parent_value: chararray);" +
                "ES_CHILD = LOAD 'pig-test-child/data' using org.elasticsearch.hadoop.pig.EsStorage() as (child_name: chararray, parent_name: chararray, child_value: long);" +
                "CO_GROUP = COGROUP ES_PARENT by parent_name, ES_CHILD by parent_name;" +
                "PARENT_CHILD = JOIN ES_PARENT by parent_name, ES_CHILD by parent_name;" +
                "STORE PARENT_CHILD INTO 'tmp-pig/testjoin-join';" +
                "STORE CO_GROUP INTO 'tmp-pig/testjoin-cogroup';";
        pig.executeScript(script);
        pig.executeScript(script2);

        String join = getResults("tmp-pig/testjoin-join");
        assertThat(join, containsString(tabify("parent1", "name1", "child1", "parent1", "100")));
        assertThat(join, containsString(tabify("parent1", "name1", "child2", "parent1", "200")));
        assertThat(join, containsString(tabify("parent2", "name2", "child3", "parent2", "300")));

        String cogroup = getResults("tmp-pig/testjoin-cogroup");
        assertThat(cogroup, containsString(tabify("parent1", "{(parent1,name1)}")));
        // bags are not ordered so check each tuple individually
        assertThat(cogroup, containsString("(child2,parent1,200)"));
        assertThat(cogroup, containsString("(child1,parent1,100)"));
        assertThat(cogroup, containsString(tabify("parent2", "{(parent2,name2)}", "{(child3,parent2,300)}")));
    }

    @Test
    public void testTemporarySchema() throws Exception {
        RestUtils.touch("pig-test-temp_schema");
        //RestUtils.putMapping("pig-test/group-data", "group-sample-mapping.txt");

        String script =
                "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                "data = LOAD 'src/itest/resources/group-sample.txt' using PigStorage(',') as (no:long,name:chararray,age:long);" +
                "data_limit = LIMIT data 1;" +
                "data_final = FOREACH data_limit GENERATE TRIM(name) as details, no as number;" +
                "STORE data_final into 'pig-test-temp_schema/data' using org.elasticsearch.hadoop.pig.EsStorage('es.mapping.id=details');";
        pig.executeScript(script);
    }

    //@Test
    public void testGroup() throws Exception {
        RestUtils.touch("pig-test-group-data-2");
        //RestUtils.putMapping("pig-test/group-data", "group-sample-mapping.txt");

        String script =
                "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                "data = LOAD 'src/itest/resources/group-sample.txt' using PigStorage(',') as (no:long,name:chararray,age:long);" +
                "data = GROUP data by $0;" +
                "data = FOREACH data GENERATE $1 as details;" +
                "DUMP data;" +
                "STORE data into 'pig-test-group-data-2/data' using org.elasticsearch.hadoop.pig.EsStorage();";
        pig.executeScript(script);
    }

    @Test
    public void testIterate() throws Exception {
        RestUtils.touch("pig-test-iterate");
        RestUtils.postData("pig-test-iterate/data", "{\"message\" : \"Hello World\",\"message_date\" : \"2014-05-25\"}".getBytes());
        RestUtils.postData("pig-test-iterate/data", "{\"message\" : \"Goodbye World\",\"message_date\" : \"2014-05-25\"}".getBytes());
        RestUtils.refresh("pig-test-iterate");

        String script =
                "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                "data = LOAD 'pig-test-iterate/data' using org.elasticsearch.hadoop.pig.EsStorage() as (message:chararray,message_date:chararray);" +
                "data = FOREACH data GENERATE message_date as date, message as message;" +
                "STORE data INTO 'tmp-pig/pig-iterate';";
        pig.executeScript(script);

        String iterate = getResults("tmp-pig/pig-iterate");
        assertThat(iterate, containsString("World"));
    }
    

    @Test
    public void testTupleSaving() throws Exception {

        String script =
                // (4,{(4,7,287),(4,7263,48)})
                // 'es.mapping.pig.tuple.use.field.names = true' -> {"group":4,"answers":[[{"id":4,"parentId":7,"score":287}],[{"id":4,"parentId":7263,"score":48}]]}
                // 'es.mapping.pig.tuple.use.field.names = false' -> {"group":4,"data":[[4,7,287],[4,7263,48]]}
                "REGISTER "+ Provisioner.ESHADOOP_TESTING_JAR + ";" +
                "answers = LOAD 'src/itest/resources/tuple.txt' using PigStorage(',') as (id:int, parentId:int, score:int);" +
                "grouped = GROUP answers by id;" +
                "ILLUSTRATE grouped;" +
                "STORE grouped into 'pig-test-tuple-structure/data' using org.elasticsearch.hadoop.pig.EsStorage('es.mapping.pig.tuple.use.field.names = true');";
        pig.executeScript(script);

        String string = RestUtils.get("pig-test-tuple-structure/data/_search?");
        assertThat(string, containsString("parentId"));
    }
}