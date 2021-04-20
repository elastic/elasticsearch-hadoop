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
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.thirdparty.apache.commons.httpclient.Header;
import org.elasticsearch.hadoop.thirdparty.apache.commons.httpclient.HttpMethod;
import org.elasticsearch.hadoop.thirdparty.apache.commons.httpclient.methods.PostMethod;
import org.elasticsearch.hadoop.util.TestSettings;
import org.junit.Test;

import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class HeaderProcessorTest {

    private static Header[] applyHeaders(Settings settings) {
        HeaderProcessor processor = new HeaderProcessor(settings);

        HttpMethod method = new PostMethod("http://localhost:9200");
        processor.applyTo(method);

        return method.getRequestHeaders();
    }

    @Test
    public void testApplyValidHeader() throws Exception {
        Settings settings = new TestSettings();
        settings.setProperty("es.net.http.header.Max-Forwards", "10");

        Header[] headers = applyHeaders(settings);

        assertThat(headers, arrayWithSize(3));
        assertThat(headers, arrayContainingInAnyOrder(
                new Header("Accept", "application/json"),
                new Header("Content-Type", "application/json"),
                new Header("Max-Forwards", "10")
        ));
    }

    @Test(expected = EsHadoopIllegalArgumentException.class)
    public void applyReservedHeader() throws Exception {
        Settings settings = new TestSettings();
        settings.setProperty("es.net.http.header.Content-Type", "application/x-ldjson");

        applyHeaders(settings);

        fail("Should not execute since we tried to set a reserved header");
    }

    @Test(expected = EsHadoopIllegalArgumentException.class)
    public void applyEmptyHeaderName() throws Exception {
        Settings settings = new TestSettings();
        settings.setProperty("es.net.http.header.", "application/x-ldjson");

        applyHeaders(settings);

        fail("Should not execute since we gave only the header prefix, and no header key");
    }

    @Test
    public void testApplyArrayValues() throws Exception {
        Settings settings = new TestSettings();
        settings.asProperties().put("es.net.http.header.Accept-Encoding", new Object[]{"gzip","deflate"});

        Header[] headers = applyHeaders(settings);

        assertThat(headers, arrayWithSize(3));
        assertThat(headers, arrayContainingInAnyOrder(
                new Header("Accept", "application/json"),
                new Header("Content-Type", "application/json"),
                new Header("Accept-Encoding", "gzip,deflate")
        ));
    }

    @Test
    public void testApplyMultiValues() throws Exception {
        Settings settings = new TestSettings();
        settings.setProperty("es.net.http.header.Accept-Encoding", "gzip,deflate");

        Header[] headers = applyHeaders(settings);

        assertThat(headers, arrayWithSize(3));
        assertThat(headers, arrayContainingInAnyOrder(
                new Header("Accept", "application/json"),
                new Header("Content-Type", "application/json"),
                new Header("Accept-Encoding", "gzip,deflate")
        ));
    }
}