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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.elasticsearch.hadoop.mr.HadoopCfgUtils;
import org.junit.rules.LazyTempFolder;

public class QueryTestParams {

    private static final String QUERY_DSL = "/org/elasticsearch/hadoop/integration/query.dsl";
    private static final String QUERY_URI = "/org/elasticsearch/hadoop/integration/query.uri";

    private final File stagingLocation;

    public QueryTestParams(LazyTempFolder temporaryFolder) {
        this.stagingLocation = temporaryFolder.getOrCreateFolder("queries");
    }

    public QueryTestParams(File stagingDir) {
        this.stagingLocation = stagingDir;
    }

    public Collection<Object[]> jsonParams() {
        return Arrays.asList(new Object[][] {
                // standard
                { "", "?q=name:mega", false, false },
                { "", "", false, false }, // empty
                { "", "?q=m*", false, false }, // uri
                { "", "?q=name:m*", false, false }, // uri
                { "", "{ \"query\" : { \"query_string\" : { \"query\":\"m*\"} } }", false, false }, // query dsl
                { "", resource(QUERY_URI), false, false }, // nested uri
                { "", resource(QUERY_DSL), false, false }, // nested dsl

                { "", "", true, false }, // empty
                { "", "?q=m*", true, false }, // uri
                { "", "?q=name:m*", true, false }, // uri
                { "", "{ \"query\" : { \"query_string\" : { \"query\":\"m*\"} } }", true, false }, // query dsl
                { "", resource(QUERY_URI), true, false }, // nested uri
                { "", resource(QUERY_DSL), true, false }, // nested dsl

                { "", "", false, true }, // empty
                { "", "?q=m*", false, true }, // uri
                { "", "?q=name:m*", false, true }, // uri
                { "", "{ \"query\" : { \"query_string\" : { \"query\":\"m*\"} } }", false, true }, // query dsl
                { "", resource(QUERY_URI), false, true }, // nested uri
                { "", resource(QUERY_DSL), false, true }, // nested dsl

                { "", "", true, true }, // empty
                { "", "?q=m*", true, true }, // uri
                { "", "?q=name:m*", true, true }, // uri
                { "", "{ \"query\" : { \"query_string\" : { \"query\":\"m*\"} } }", true, true }, // query dsl
                { "", resource(QUERY_URI), true, true }, // nested uri
                { "", resource(QUERY_DSL), true, true }, // nested dsl

                // json
                { "json-", "", false, false }, // empty
                { "json-", "?q=m*", false, false }, // uri
                { "json-", "?q=name:m*", false, false }, // uri
                { "json-", "{ \"query\" : { \"query_string\" : { \"query\":\"m*\"} } }", false, false }, // query dsl
                { "json-", resource(QUERY_URI), false, false }, // nested uri
                { "json-", resource(QUERY_DSL), false, false }, // nested dsl

                // json
                { "json-", "", true, false }, // empty
                { "json-", "?q=m*", true, false }, // uri
                { "json-", "?q=name:m*", true, false }, // uri
                { "json-", "{ \"query\" : { \"query_string\" : { \"query\":\"m*\"} } }", true, false }, // query dsl
                { "json-", resource(QUERY_URI), true, false }, // nested uri
                { "json-", resource(QUERY_DSL), true, false }, // nested dsl

                // json
                { "json-", "", false, true }, // empty
                { "json-", "?q=m*", false, true }, // uri
                { "json-", "?q=name:m*", false, true }, // uri
                { "json-", "{ \"query\" : { \"query_string\" : { \"query\":\"m*\"} } }", false, true }, // query dsl
                { "json-", resource(QUERY_URI), false, true }, // nested uri
                { "json-", resource(QUERY_DSL), false, true }, // nested dsl

                // json
                { "json-", "", true, true }, // empty
                { "json-", "?q=m*", true, true }, // uri
                { "json-", "?q=name:m*", true, true }, // uri
                { "json-", "{ \"query\" : { \"query_string\" : { \"query\":\"m*\"} } }", true, true }, // query dsl
                { "json-", resource(QUERY_URI), true, true }, // nested uri
                { "json-", resource(QUERY_DSL), true, true } // nested dsl

        });
    }

    public Collection<Object[]> jsonLocalParams() {
        return Arrays.asList(new Object[][] {
                { "", "" }, // empty
                { "", "?q=m*" }, // uri
                { "", "?q=name:m*" }, // uri
                { "", "{ \"query\" : { \"query_string\" : { \"query\":\"m*\"} } }" }, // query dsl
                { "", resource(QUERY_URI) }, // nested uri
                { "", resource(QUERY_DSL) }, // nested dsl

                { "json-", "" }, // empty
                { "json-", "?q=m*" }, // uri
                { "json-", "?q=name:m*" }, // uri
                { "json-", "{ \"query\" : { \"query_string\" : { \"query\":\"m*\"} } }" }, // query dsl
                { "json-", resource(QUERY_URI) }, // nested uri
                { "json-", resource(QUERY_DSL) } // nested dsl
                });
    }

    public Collection<Object[]> params() {
        return Arrays.asList(new Object[][] {
                { "", true }, // empty
                { "?q=m*", true }, // uri
                //{ "?q=@name:m*", true }, // uri
                { "{ \"query\" : { \"query_string\" : { \"query\":\"m*\"} } }", true }, // query dsl
                { resource(QUERY_URI), true }, // nested uri
                { resource(QUERY_DSL), true }, // nested dsl

                { "", false }, // empty
                { "?q=m*", false }, // uri
                //{ "?q=name:m*", false }, // uri
                { "{ \"query\" : { \"query_string\" : { \"query\":\"m*\"} } }", false }, // query dsl
                { resource(QUERY_URI), false }, // nested uri
                { resource(QUERY_DSL), false } // nested dsl
        });
    }

    public Collection<Object[]> localParams() {
        return Arrays.asList(new Object[][] {
                { "", true }, // empty
                { "?q=m*", true }, // uri
                { "?q=name:m*", true }, // uri
                { "{ \"query\" : { \"query_string\" : { \"query\":\"m*\"} } }", true }, // query dsl
                { resource(QUERY_URI), true }, // nested uri
                { resource(QUERY_DSL), true }, // nested dsl

                { "", false }, // empty
                { "?q=m*", false }, // uri
                { "?q=name:m*", false }, // uri
                { "{ \"query\" : { \"query_string\" : { \"query\":\"m*\"} } }", false }, // query dsl
                { resource(QUERY_URI), false }, // nested uri
                { resource(QUERY_DSL), false } // nested dsl
                });
    }

    private String resource(String resource) {
        return TestData.unpackResource(resource, stagingLocation).toURI().toString();
    }

    @SuppressWarnings("deprecation")
    public <T extends Configuration> T provisionQueries(T cfg) {
        if (HadoopCfgUtils.isLocal(cfg)) {
            return cfg;
        }

        try {
            DistributedCache.addFileToClassPath(new Path(TestData.unpackResource(QUERY_DSL, stagingLocation).getAbsolutePath()), cfg);
            DistributedCache.addFileToClassPath(new Path(TestData.unpackResource(QUERY_URI, stagingLocation).getAbsolutePath()), cfg);
        } catch (IOException ex) {
        }
        return cfg;
    }
}
