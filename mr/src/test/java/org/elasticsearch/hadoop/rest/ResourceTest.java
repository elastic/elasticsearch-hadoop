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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.util.EsMajorVersion;
import org.elasticsearch.hadoop.util.TestSettings;
import org.elasticsearch.hadoop.util.encoding.HttpEncodingTools;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class ResourceTest {

    @Parameters
    public static Collection<Object[]> params() {
        List<Object[]> parameters = new ArrayList<Object[]>();
        parameters.add(new Object[]{EsMajorVersion.LATEST, true});
        parameters.add(new Object[]{EsMajorVersion.LATEST, false});

        parameters.add(new Object[]{EsMajorVersion.V_8_X, true});
        parameters.add(new Object[]{EsMajorVersion.V_7_X, true});
        parameters.add(new Object[]{EsMajorVersion.V_6_X, true});
        parameters.add(new Object[]{EsMajorVersion.V_5_X, true});
        parameters.add(new Object[]{EsMajorVersion.V_2_X, true});

        parameters.add(new Object[]{EsMajorVersion.V_8_X, false});
        parameters.add(new Object[]{EsMajorVersion.V_7_X, false});
        parameters.add(new Object[]{EsMajorVersion.V_6_X, false});
        parameters.add(new Object[]{EsMajorVersion.V_5_X, false});
        parameters.add(new Object[]{EsMajorVersion.V_2_X, false});
        return parameters;
    }

    private final EsMajorVersion testVersion;
    private final boolean readResource;

    public ResourceTest(EsMajorVersion testVersion, boolean readResource) {
        this.testVersion = testVersion;
        this.readResource = readResource;
    }

    @Test
    public void testToString() throws Exception {
        assumeTyped();
        Resource res = createResource("foo/bar");
        assertEquals("foo/bar", res.toString());
    }

    @Test
    public void testJustIndex() throws Exception {
        assumeTyped();
        Resource res = createResource("foo/_all");
        assertEquals("foo/_all", res.toString());
    }

    @Test
    public void testJustType() throws Exception {
        assumeTyped();
        Resource res = createResource("_all/foo");
        assertEquals("_all/foo", res.toString());
    }

    @Test
    public void testAllTypeless() throws Exception {
        assumeTypeless();
        Resource res = createResource("_all");
        assertEquals("_all", res.toString());
    }

    @Test
    public void testIndexAndType() throws Exception {
        assumeTyped();
        Resource res = createResource("foo/bar");
        assertEquals("foo/bar", res.toString());
    }

    @Test
    public void testIndexTypeless() throws Exception {
        assumeTypeless();
        Resource res = createResource("foo");
        assertEquals("foo", res.toString());
    }

    @Test
    public void testUnderscore() throws Exception {
        assumeTyped();
        Resource res = createResource("fo_o/ba_r");
        assertEquals("fo_o/ba_r", res.toString());
    }

    @Test
    public void testUnderscoreTypeless() throws Exception {
        assumeTypeless();
        Resource res = createResource("fo_o");
        assertEquals("fo_o", res.toString());
    }

    @Test
    public void testQueryUri() throws Exception {
        assumeTyped();
        Settings s = new TestSettings();
        Resource res = createResource("foo/bar/_search=?somequery", s);
        assertEquals("foo/bar", res.toString());
        assertEquals("?somequery", s.getQuery());
    }

    @Test
    public void testQueryUriTypeless() throws Exception {
        assumeTypeless();
        Settings s = new TestSettings();
        Resource res = createResource("foo/_search=?somequery", s);
        assertEquals("foo", res.toString());
        assertEquals("?somequery", s.getQuery());
    }

    @Test
    public void testQueryUriWithParams() throws Exception {
        assumeTyped();
        Settings s = new TestSettings();
        Resource res = createResource("foo/bar/_search=?somequery&bla=bla", s);
        assertEquals("foo/bar", res.toString());
        assertEquals("?somequery&bla=bla", s.getQuery());
    }

    @Test
    public void testQueryUriWithParamsTypeless() throws Exception {
        assumeTypeless();
        Settings s = new TestSettings();
        Resource res = createResource("foo/_search=?somequery&bla=bla", s);
        assertEquals("foo", res.toString());
        assertEquals("?somequery&bla=bla", s.getQuery());
    }

    @Test(expected = EsHadoopIllegalArgumentException.class)
    public void testQueryUriConflict() throws Exception {
        assumeTyped();
        Settings s = new TestSettings();
        s.setProperty(ConfigurationOptions.ES_QUERY, "{\"match_all\":{}}");
        Resource res = createResource("foo/bar/_search=?somequery", s);
        assertEquals("foo/bar", res.toString());
    }

    @Test(expected = EsHadoopIllegalArgumentException.class)
    public void testQueryUriConflictTypeless() throws Exception {
        assumeTypeless();
        Settings s = new TestSettings();
        s.setProperty(ConfigurationOptions.ES_QUERY, "{\"match_all\":{}}");
        Resource res = createResource("foo/_search=?somequery", s);
        assertEquals("foo", res.toString());
    }

    @Test(expected = EsHadoopIllegalArgumentException.class)
    public void testQueryUriConflictWithParams() throws Exception {
        assumeTyped();
        Settings s = new TestSettings();
        s.setProperty(ConfigurationOptions.ES_QUERY, "{\"match_all\":{}}");
        Resource res = createResource("foo/bar/_search=?somequery&bla=bla", s);
        assertEquals("foo/bar", res.toString());
    }

    @Test(expected = EsHadoopIllegalArgumentException.class)
    public void testQueryUriConflictWithParamsTypeless() throws Exception {
        assumeTypeless();
        Settings s = new TestSettings();
        s.setProperty(ConfigurationOptions.ES_QUERY, "{\"match_all\":{}}");
        Resource res = createResource("foo/_search=?somequery&bla=bla", s);
        assertEquals("foo", res.toString());
    }

    @Test
    public void testDynamicFieldLowercase() throws Exception {
        assumeTyped();
        Resource res = createResource("foo/Fbar");
        res = createResource("foo-{F}/bar");
    }

    @Test
    public void testDynamicFieldTypeless() throws Exception {
        assumeTypeless();
        Resource res = createResource("foo");
        res = createResource("foo-{F}");
    }

    @Test(expected = EsHadoopIllegalArgumentException.class)
    public void testNoWhitespaceAllowed() throws Exception {
        assumeTyped();
        createResource("foo, bar/far");
    }

    @Test(expected = EsHadoopIllegalArgumentException.class)
    public void testNoWhitespaceAllowedTypeless() throws Exception {
        assumeTypeless();
        createResource("foo, bar");
    }

    @Test
    public void testBulkWithIngestPipeline() throws Exception {
        assumeTyped();
        Settings settings = new TestSettings();
        settings.setProperty(ConfigurationOptions.ES_INGEST_PIPELINE, "ingest-pipeline");
        Resource res = createResource("pipeline/test", settings);
        assertEquals("pipeline/test", res.toString());
        assertEquals("pipeline/_aliases", res.aliases());
        assertEquals("pipeline/test/_bulk?pipeline=ingest-pipeline", res.bulk());
        assertEquals("pipeline/_refresh", res.refresh());
    }

    @Test
    public void testBulkWithIngestPipelineTypeless() throws Exception {
        assumeTypeless();
        Settings settings = new TestSettings();
        settings.setProperty(ConfigurationOptions.ES_INGEST_PIPELINE, "ingest-pipeline");
        Resource res = createResource("pipeline", settings);
        assertEquals("pipeline", res.toString());
        assertEquals("pipeline/_aliases", res.aliases());
        assertEquals("pipeline/_bulk?pipeline=ingest-pipeline", res.bulk());
        assertEquals("pipeline/_refresh", res.refresh());
    }


    @Test(expected = EsHadoopIllegalArgumentException.class)
    public void testBulkWithBadIngestPipeline() throws Exception {
        assumeTyped();
        Settings settings = new TestSettings();
        settings.setProperty(ConfigurationOptions.ES_INGEST_PIPELINE, "ingest pipeline");
        createResource("pipeline/test", settings);
    }

    @Test(expected = EsHadoopIllegalArgumentException.class)
    public void testBulkUpdateBreaksWithIngestPipeline() throws Exception {
        assumeTyped();
        Settings settings = new TestSettings();
        settings.setProperty(ConfigurationOptions.ES_INGEST_PIPELINE, "ingest-pipeline");
        settings.setProperty(ConfigurationOptions.ES_WRITE_OPERATION, ConfigurationOptions.ES_OPERATION_UPDATE);
        createResource("pipeline/test", settings);
    }

    @Test(expected = EsHadoopIllegalArgumentException.class)
    public void testBulkUpsertBreaksWithIngestPipeline() throws Exception {
        assumeTyped();
        Settings settings = new TestSettings();
        settings.setProperty(ConfigurationOptions.ES_INGEST_PIPELINE, "ingest-pipeline");
        settings.setProperty(ConfigurationOptions.ES_WRITE_OPERATION, ConfigurationOptions.ES_OPERATION_UPSERT);
        createResource("pipeline/test", settings);
    }

    private Resource createResource(String target) {
        return createResource(target, new TestSettings());
    }

    private Resource createResource(String target, Settings s) {
        s.setInternalVersion(testVersion);
        s.setProperty(ConfigurationOptions.ES_RESOURCE, target);
        return new Resource(s, true);
    }

    private void assumeTyped() {
        Assume.assumeTrue("Typed api only accepted 7.X and before. Running [" + testVersion + ", " + readResource + "]",
                (testVersion.onOrBefore(EsMajorVersion.V_7_X)));
    }

    private void assumeTypeless() {
        Assume.assumeTrue("Typeless api only accepted 7.X and up for writes. Running [" + testVersion + ", " + readResource + "]",
                (testVersion.onOrAfter(EsMajorVersion.V_7_X) || readResource));
    }

    @Test
    public void testURiEscaping() throws Exception {
        assertEquals("http://localhost:9200/index/type%7Cfoo?q=foo%7Cbar:bar%7Cfoo", HttpEncodingTools.encodeUri("http://localhost:9200/index/type|foo?q=foo|bar:bar|foo"));
        assertEquals("foo%7Cbar", HttpEncodingTools.encodeUri("foo|bar"));
    }
}
