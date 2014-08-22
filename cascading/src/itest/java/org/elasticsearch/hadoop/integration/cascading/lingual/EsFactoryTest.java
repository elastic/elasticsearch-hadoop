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
package org.elasticsearch.hadoop.integration.cascading.lingual;

import java.io.IOException;
import java.util.Properties;

import org.elasticsearch.hadoop.cascading.lingual.EsFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;

import static org.junit.Assert.*;

import static org.hamcrest.Matchers.*;

public class EsFactoryTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private EsFactory factory = new EsFactory();

    @Test
    public void testCreateScheme() throws IOException {
        Scheme scheme = factory.createScheme(new Fields(), new Properties());
        assertThat(scheme, notNullValue());

        try {
            scheme.sourceConfInit(null, null, null);
            fail();
        } catch (UnsupportedOperationException ex) {
        }

        try {
            scheme.sinkConfInit(null, null, null);
            fail();
        } catch (UnsupportedOperationException ex) {
        }

        try {
            scheme.source(null, null);
            fail();
        } catch (UnsupportedOperationException ex) {
        }
        try {
            scheme.sink(null, null);
            fail();
        } catch (UnsupportedOperationException ex) {
        }

    }

    @Test
    public void testCreateTap() {
        Fields fl = new Fields();
        Properties props = new Properties();

        Scheme scheme = factory.createScheme(fl, props);
        Tap tap = factory.createTap(scheme, "somePath", SinkMode.KEEP, props);
        assertThat(tap, notNullValue());
        assertThat(tap.getClass().getName(), containsString("HadoopTap"));
    }
}
