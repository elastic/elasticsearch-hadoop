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
package org.elasticsearch.hadoop;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.elasticsearch.hadoop.mr.HadoopCfgUtils;
import org.elasticsearch.hadoop.util.TestUtils;

public class QueryTestParams {

    public static Collection<Object[]> jsonParams() {
        return Arrays.asList(new Object[][] {
                // standard
                { "", "?q=name:mega", false, false },
                { "", "", false, false }, // empty
                { "", "?q=m*", false, false }, // uri
                { "", "?q=name:m*", false, false }, // uri
                { "", "{ \"query\" : { \"query_string\" : { \"query\":\"m*\"} } }", false, false }, // query dsl
                { "", TestUtils.sampleQueryUri(), false, false }, // nested uri
                { "", TestUtils.sampleQueryDsl(), false, false }, // nested dsl

                { "", "", true, false }, // empty
                { "", "?q=m*", true, false }, // uri
                { "", "?q=name:m*", true, false }, // uri
                { "", "{ \"query\" : { \"query_string\" : { \"query\":\"m*\"} } }", true, false }, // query dsl
                { "", TestUtils.sampleQueryUri(), true, false }, // nested uri
                { "", TestUtils.sampleQueryDsl(), true, false }, // nested dsl

                { "", "", false, true }, // empty
                { "", "?q=m*", false, true }, // uri
                { "", "?q=name:m*", false, true }, // uri
                { "", "{ \"query\" : { \"query_string\" : { \"query\":\"m*\"} } }", false, true }, // query dsl
                { "", TestUtils.sampleQueryUri(), false, true }, // nested uri
                { "", TestUtils.sampleQueryDsl(), false, true }, // nested dsl

                { "", "", true, true }, // empty
                { "", "?q=m*", true, true }, // uri
                { "", "?q=name:m*", true, true }, // uri
                { "", "{ \"query\" : { \"query_string\" : { \"query\":\"m*\"} } }", true, true }, // query dsl
                { "", TestUtils.sampleQueryUri(), true, true }, // nested uri
                { "", TestUtils.sampleQueryDsl(), true, true }, // nested dsl

                // json
                { "json-", "", false, false }, // empty
                { "json-", "?q=m*", false, false }, // uri
                { "json-", "?q=name:m*", false, false }, // uri
                { "json-", "{ \"query\" : { \"query_string\" : { \"query\":\"m*\"} } }", false, false }, // query dsl
                { "json-", TestUtils.sampleQueryUri(), false, false }, // nested uri
                { "json-", TestUtils.sampleQueryDsl(), false, false }, // nested dsl

                // json
                { "json-", "", true, false }, // empty
                { "json-", "?q=m*", true, false }, // uri
                { "json-", "?q=name:m*", true, false }, // uri
                { "json-", "{ \"query\" : { \"query_string\" : { \"query\":\"m*\"} } }", true, false }, // query dsl
                { "json-", TestUtils.sampleQueryUri(), true, false }, // nested uri
                { "json-", TestUtils.sampleQueryDsl(), true, false }, // nested dsl

                // json
                { "json-", "", false, true }, // empty
                { "json-", "?q=m*", false, true }, // uri
                { "json-", "?q=name:m*", false, true }, // uri
                { "json-", "{ \"query\" : { \"query_string\" : { \"query\":\"m*\"} } }", false, true }, // query dsl
                { "json-", TestUtils.sampleQueryUri(), false, true }, // nested uri
                { "json-", TestUtils.sampleQueryDsl(), false, true }, // nested dsl

                // json
                { "json-", "", true, true }, // empty
                { "json-", "?q=m*", true, true }, // uri
                { "json-", "?q=name:m*", true, true }, // uri
                { "json-", "{ \"query\" : { \"query_string\" : { \"query\":\"m*\"} } }", true, true }, // query dsl
                { "json-", TestUtils.sampleQueryUri(), true, true }, // nested uri
                { "json-", TestUtils.sampleQueryDsl(), true, true } // nested dsl

        });
    }

    public static Collection<Object[]> jsonLocalParams() {
        return Arrays.asList(new Object[][] {
                { "", "" }, // empty
                { "", "?q=m*" }, // uri
                { "", "?q=name:m*" }, // uri
                { "", "{ \"query\" : { \"query_string\" : { \"query\":\"m*\"} } }" }, // query dsl
                { "", TestUtils.sampleQueryUri() }, // nested uri
                { "", TestUtils.sampleQueryDsl() }, // nested dsl

                { "json-", "" }, // empty
                { "json-", "?q=m*" }, // uri
                { "json-", "?q=name:m*" }, // uri
                { "json-", "{ \"query\" : { \"query_string\" : { \"query\":\"m*\"} } }" }, // query dsl
                { "json-", TestUtils.sampleQueryUri() }, // nested uri
                { "json-", TestUtils.sampleQueryDsl() } // nested dsl
                });
    }

    public static Collection<Object[]> params() {
        return Arrays.asList(new Object[][] {
                { "", true }, // empty
                { "?q=m*", true }, // uri
                //{ "?q=@name:m*", true }, // uri
                { "{ \"query\" : { \"query_string\" : { \"query\":\"m*\"} } }", true }, // query dsl
                { TestUtils.sampleQueryUri(), true }, // nested uri
                { TestUtils.sampleQueryDsl(), true }, // nested dsl

                { "", false }, // empty
                { "?q=m*", false }, // uri
                //{ "?q=name:m*", false }, // uri
                { "{ \"query\" : { \"query_string\" : { \"query\":\"m*\"} } }", false }, // query dsl
                { TestUtils.sampleQueryUri(), false }, // nested uri
                { TestUtils.sampleQueryDsl(), false } // nested dsl
        });
    }

    public static Collection<Object[]> localParams() {
        return Arrays.asList(new Object[][] {
                { "", true }, // empty
                { "?q=m*", true }, // uri
                { "?q=name:m*", true }, // uri
                { "{ \"query\" : { \"query_string\" : { \"query\":\"m*\"} } }", true }, // query dsl
                { TestUtils.sampleQueryUri(), true }, // nested uri
                { TestUtils.sampleQueryDsl(), true }, // nested dsl

                { "", false }, // empty
                { "?q=m*", false }, // uri
                { "?q=name:m*", false }, // uri
                { "{ \"query\" : { \"query_string\" : { \"query\":\"m*\"} } }", false }, // query dsl
                { TestUtils.sampleQueryUri(), false }, // nested uri
                { TestUtils.sampleQueryDsl(), false } // nested dsl
                });
    }

    @SuppressWarnings("deprecation")
    public static <T extends Configuration> T provisionQueries(T cfg) {
        if (HadoopCfgUtils.isLocal(cfg)) {
            return cfg;
        }

        try {
            DistributedCache.addFileToClassPath(new Path(TestUtils.sampleQueryDsl()), cfg);
            DistributedCache.addFileToClassPath(new Path(TestUtils.sampleQueryUri()), cfg);
        } catch (IOException ex) {
        }
        return cfg;
    }
}
