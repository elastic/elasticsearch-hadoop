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
import org.elasticsearch.hadoop.util.TestSettings;
import org.elasticsearch.hadoop.util.encoding.HttpEncodingTools;
import org.junit.Test;

import static org.junit.Assert.*;

public class ResourceTest {

    @Test
    public void testJustIndex() throws Exception {
        Resource res = createResource("foo/_all");
        assertEquals("foo/_all", res.toString());
    }

    @Test
    public void testJustType() throws Exception {
        Resource res = createResource("_all/foo");
        assertEquals("_all/foo", res.toString());
    }

    @Test
    public void testIndexAndType() throws Exception {
        Resource res = createResource("foo/bar");
        assertEquals("foo/bar", res.toString());
    }

    @Test
    public void testUnderscore() throws Exception {
        Resource res = createResource("fo_o/ba_r");
        assertEquals("fo_o/ba_r", res.toString());
    }

    @Test
    public void testQueryUri() throws Exception {
        Settings s = new TestSettings();
        Resource res = createResource("foo/bar/_search=?somequery", s);
        assertEquals("foo/bar", res.toString());
        assertEquals("?somequery", s.getQuery());
    }

    @Test
    public void testQueryUriWithParams() throws Exception {
        Settings s = new TestSettings();
        Resource res = createResource("foo/bar/_search=?somequery&bla=bla", s);
        assertEquals("foo/bar", res.toString());
        assertEquals("?somequery&bla=bla", s.getQuery());
    }

    @Test(expected = EsHadoopIllegalArgumentException.class)
    public void testQueryUriConflict() throws Exception {
        Settings s = new TestSettings();
        s.setProperty(ConfigurationOptions.ES_QUERY, "{\"match_all\":{}}");
        Resource res = createResource("foo/bar/_search=?somequery", s);
        assertEquals("foo/bar", res.toString());
    }

    @Test(expected = EsHadoopIllegalArgumentException.class)
    public void testQueryUriConflictWithParams() throws Exception {
        Settings s = new TestSettings();
        s.setProperty(ConfigurationOptions.ES_QUERY, "{\"match_all\":{}}");
        Resource res = createResource("foo/bar/_search=?somequery&bla=bla", s);
        assertEquals("foo/bar", res.toString());
    }

    @Test
    public void testDynamicFieldLowercase() throws Exception {
        Resource res = createResource("foo/Fbar");
        res = createResource("foo-{F}/bar");
    }

    @Test//(expected = EsHadoopIllegalArgumentException.class)
    public void testLowercaseNotAllowed() throws Exception {
        createResource("fooF/bar");
    }

    @Test//(expected = EsHadoopIllegalArgumentException.class)
    public void testLowercaseNotAllowedIfTemplateIsInvalid() throws Exception {
        createResource("foo{F/bar");
    }

    @Test//(expected = EsHadoopIllegalArgumentException.class)
    public void testLowercaseNotAllowedIfTemplateIsInvalidAgain() throws Exception {
        createResource("foo}F{/bar");
    }

    @Test(expected = EsHadoopIllegalArgumentException.class)
    public void testNoWhitespaceAllowed() throws Exception {
        createResource("foo, bar/far");
    }

    @Test
    public void testBulkWithIngestPipeline() throws Exception {
        Settings settings = new TestSettings();
        settings.setProperty(ConfigurationOptions.ES_INGEST_PIPELINE, "ingest-pipeline");
        Resource res = createResource("pipeline/test", settings);
        assertEquals("pipeline/test", res.toString());
        assertEquals("pipeline/test/_mapping", res.mapping());
        assertEquals("pipeline/_aliases", res.aliases());
        assertEquals("pipeline/test/_bulk?pipeline=ingest-pipeline", res.bulk());
        assertEquals("pipeline/_refresh", res.refresh());
    }


    @Test(expected = EsHadoopIllegalArgumentException.class)
    public void testBulkWithBadIngestPipeline() throws Exception {
        Settings settings = new TestSettings();
        settings.setProperty(ConfigurationOptions.ES_INGEST_PIPELINE, "ingest pipeline");
        createResource("pipeline/test", settings);
    }

    @Test(expected = EsHadoopIllegalArgumentException.class)
    public void testBulkUpdateBreaksWithIngestPipeline() throws Exception {
        Settings settings = new TestSettings();
        settings.setProperty(ConfigurationOptions.ES_INGEST_PIPELINE, "ingest-pipeline");
        settings.setProperty(ConfigurationOptions.ES_WRITE_OPERATION, ConfigurationOptions.ES_OPERATION_UPDATE);
        createResource("pipeline/test", settings);
    }

    @Test(expected = EsHadoopIllegalArgumentException.class)
    public void testBulkUpsertBreaksWithIngestPipeline() throws Exception {
        Settings settings = new TestSettings();
        settings.setProperty(ConfigurationOptions.ES_INGEST_PIPELINE, "ingest-pipeline");
        settings.setProperty(ConfigurationOptions.ES_WRITE_OPERATION, ConfigurationOptions.ES_OPERATION_UPSERT);
        createResource("pipeline/test", settings);
    }

    private Resource createResource(String target) {
        return createResource(target, new TestSettings());
    }

    private Resource createResource(String target, Settings s) {
        s.setProperty(ConfigurationOptions.ES_RESOURCE, target);
        return new Resource(s, true);
    }

    @Test
    public void testURiEscaping() throws Exception {
        assertEquals("http://localhost:9200/index/type%7Cfoo?q=foo%7Cbar:bar%7Cfoo", HttpEncodingTools.encodeUri("http://localhost:9200/index/type|foo?q=foo|bar:bar|foo"));
        assertEquals("foo%7Cbar", HttpEncodingTools.encodeUri("foo|bar"));
        System.out.println(HttpEncodingTools.encodeUri("foo|bar,abc,xyz|rpt"));
    }
}
