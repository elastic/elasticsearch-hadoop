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
package org.elasticsearch.hadoop.integration.cascading;

import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import org.elasticsearch.hadoop.cascading.ESTap;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.util.TestSettings;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import cascading.flow.local.LocalFlowConnector;
import cascading.operation.aggregator.Count;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.local.TextLine;
import cascading.tap.Tap;
import cascading.tap.local.StdOutTap;

@RunWith(Parameterized.class)
public class CascadingLocalSearchTest {

    @Parameters
    public static Collection<Object[]> queries() {
        return Arrays.asList(new Object[][] { { "" }, { "?q=me*" },
                { "{ \"query\" : { \"query_string\" : { \"query\":\"me*\"} } }" } });
    }

    private String query;

    public CascadingLocalSearchTest(String query) {
        this.query = query;
    }

    @Test
    public void testReadFromES() throws Exception {
        Tap in = new ESTap("cascading-local/artists");
        Pipe pipe = new Pipe("copy");
        pipe = new GroupBy(pipe);
        pipe = new Every(pipe, new Count());

        // print out
        StdOutTap out = new StdOutTap(new TextLine());
        new LocalFlowConnector(cfg()).connect(in, out, pipe).complete();
    }

    private Properties cfg() {
        Properties props = new TestSettings().getProperties();
        props.put(ConfigurationOptions.ES_QUERY, query);
        return props;
    }
}
