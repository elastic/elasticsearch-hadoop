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

package org.elasticsearch.hadoop.fixtures;

import java.io.File;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.minikdc.MiniKdc;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

public class KDCFixture extends ExternalResource {

    private TemporaryFolder temporaryFolder;
    private MiniKdc kdc;

    public KDCFixture(TemporaryFolder temporaryFolder) {
        this.temporaryFolder = temporaryFolder;
    }

    @Override
    protected void before() throws Throwable {
        Properties conf = MiniKdc.createConf();
        conf.setProperty(MiniKdc.ORG_NAME, "BUILD.ELASTIC");
        conf.setProperty(MiniKdc.ORG_DOMAIN, "CO");
        kdc = new MiniKdc(conf, temporaryFolder.newFolder());
        kdc.start();
    }

    public void createPrincipal(String principal, String password) throws Exception {
        kdc.createPrincipal(principal, password);
    }

    @Test
    public void createPrincipal(File keytab, List<String> principals) throws Exception {
        kdc.createPrincipal(keytab, principals.toArray(new String[0]));
    }

    @Test
    public void createPrincipal(File keytab, String... principals) throws Exception {
        kdc.createPrincipal(keytab, principals);
    }

    @Override
    protected void after() {
        kdc.stop();
    }
}
