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
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.util.StringUtils;
import org.elasticsearch.hadoop.util.TestSettings;
import org.junit.Test;

import static org.junit.Assert.*;

public class ResourceTest {

    @Test
    public void testJustIndex() throws Exception {
        Resource res = createResource("foo/_all");
        assertEquals("foo/_all", res.indexAndType());
    }

    @Test
    public void testJustType() throws Exception {
        Resource res = createResource("_all/foo");
        assertEquals("_all/foo", res.indexAndType());
    }

    @Test
    public void testIndexAndType() throws Exception {
        Resource res = createResource("foo/bar");
        assertEquals("foo/bar", res.indexAndType());
    }

    @Test
    public void testUnderscore() throws Exception {
        Resource res = createResource("fo_o/ba_r");
        assertEquals("fo_o/ba_r", res.indexAndType());
    }

    @Test(expected = EsHadoopIllegalArgumentException.class)
    public void testQueryUri() throws Exception {
        Resource res = createResource("foo/bar/_search=?somequery");
        assertEquals("foo/bar", res.indexAndType());
    }

    @Test(expected = EsHadoopIllegalArgumentException.class)
    public void testQueryUriWithParams() throws Exception {
        Resource res = createResource("foo/bar/_search=?somequery&bla=bla");
        assertEquals("foo/bar", res.indexAndType());
    }

    @Test
    public void testDynamicFieldLowercase() throws Exception {
        Resource res = createResource("foo/Fbar");
        res = createResource("foo-{F}/bar");
    }

    @Test(expected = EsHadoopIllegalArgumentException.class)
    public void testLowercaseNotAllowed() throws Exception {
        createResource("fooF/bar");
    }

    @Test(expected = EsHadoopIllegalArgumentException.class)
    public void testLowercaseNotAllowedIfTemplateIsInvalid() throws Exception {
        createResource("foo{F/bar");
    }

    @Test(expected = EsHadoopIllegalArgumentException.class)
    public void testLowercaseNotAllowedIfTemplateIsInvalidAgain() throws Exception {
        createResource("foo}F{/bar");
    }

    private Resource createResource(String target) {
        Settings s = new TestSettings();
        s.setProperty(ConfigurationOptions.ES_RESOURCE, target);
        return new Resource(s, true);
    }

    @Test
    public void testURiEscaping() throws Exception {
        assertEquals("http://localhost:9200/index/type%7Cfoo?q=foo%7Cbar:bar%7Cfoo", StringUtils.encodeUri("http://localhost:9200/index/type|foo?q=foo|bar:bar|foo"));
        assertEquals("foo%7Cbar", StringUtils.encodeUri("foo|bar"));
        System.out.println(StringUtils.encodeUri("foo|bar,abc,xyz|rpt"));
    }
}
