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
package org.elasticsearch.hadoop.cascading;

import org.elasticsearch.hadoop.EmbeddedElasticsearchServer;
import org.elasticsearch.hadoop.util.TestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.Identity;
import cascading.operation.filter.FilterNull;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Lfs;
import cascading.tuple.Fields;

public class CascadingHadoopTest {

    private static EmbeddedElasticsearchServer esServer;

    {
        TestUtils.hackHadoopStagingOnWin();
    }

    @BeforeClass
    public static void beforeClass() {
      esServer = new EmbeddedElasticsearchServer();
    }

    @AfterClass
    public static void afterClass() {
      esServer.shutdown();
    }

    @Test
    public void testWriteToESAdnReadFromES() throws Exception {        
        testWriteToES();
      
        testReadFromES();      
    }

    private void testWriteToES() throws Exception {
        // local file-system source
        Tap in = new Lfs(new TextDelimited(new Fields("id", "name", "url", "picture")), "src/test/resources/artists.dat");
        Tap out = new ESTap("billboard/artists", new Fields("name", "url", "picture"));
        Pipe pipe = new Pipe("copy");

        // filter null properties
        pipe = new Each(pipe, new Fields("id", "name", "url", "picture"), new FilterNull());

        // rename "id" -> "garbage"
        pipe = new Each(pipe, new Identity(new Fields("garbage", "name", "url", "picture")));
        new HadoopFlowConnector().connect(in, out, pipe).complete();
    }

    public void testReadFromES() throws Exception {
        Tap in = new ESTap("http://localhost:9200/billboard/artists/_search?q=me*");
        Pipe copy = new Pipe("copy");
        // print out
        Tap out = new HadoopStdOutTap();

        new HadoopFlowConnector().connect(in, out, copy).complete();
    }
}
