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

import org.junit.Test;

import cascading.flow.local.LocalFlowConnector;
import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.local.TextDelimited;
import cascading.scheme.local.TextLine;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tap.local.StdOutTap;
import cascading.tuple.Fields;

public class CascadingLocalTest {

    @Test
    public void testWriteToES() throws Exception {
        // local file-system source
        Tap in = new FileTap(new TextDelimited(new Fields("id", "name", "url", "picture")), "src/test/resources/artists.dat");
        Tap out = new ESTap("top/artists", new Fields("name", "url", "picture"));

        Pipe pipe = new Pipe("copy");

        // rename "id" -> "garbage"
        pipe = new Each(pipe, new Identity(new Fields("garbage", "name", "url", "picture")));
        new LocalFlowConnector().connect(in, out, pipe).complete();
    }

    @Test
    public void testReadFromES() throws Exception {
        Tap in = new ESTap("top/artists/_search?q=me*");
        Pipe copy = new Pipe("copy");
        // print out
        StdOutTap out = new StdOutTap(new TextLine());
        new LocalFlowConnector().connect(in, out, copy).complete();
    }
}
