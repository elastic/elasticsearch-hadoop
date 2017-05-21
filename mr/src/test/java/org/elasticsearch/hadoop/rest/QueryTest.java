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
package org.elasticsearch.hadoop.rest;

import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.util.EsMajorVersion;
import org.elasticsearch.hadoop.util.TestSettings;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class QueryTest {
    private TestSettings cfg;
    private SearchRequestBuilder builder;

    @Before
    public void setup() {
        cfg = new TestSettings();
        builder = new SearchRequestBuilder(EsMajorVersion.V_5_X, true);
    }

    @Test
    public void testSimpleQuery() {
        cfg.setResourceRead("foo/bar");
        assertTrue(builder.indices("foo").types("bar").toString().contains("foo/bar"));
    }

    @Test
    public void testExcludeSourceTrue() {
        assertTrue(builder.excludeSource(true).toString().contains("_source=false"));
    }

    @Test
    public void testExcludeSourceFalse() {
        assertFalse(builder.fields("a,b").excludeSource(false).toString().contains("_source=false"));
    }

    @Test(expected=EsHadoopIllegalArgumentException.class)
    public void testExcludeSourceAndGetFields() {
        builder.fields("a,b").excludeSource(true);
    }

    @Test(expected=EsHadoopIllegalArgumentException.class)
    public void testGetFieldsAndExcludeSource() {
        builder.excludeSource(true).fields("a,b");
    }
}
