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

package org.elasticsearch.hadoop.rest.commonshttp.auth.spnego;

import java.io.File;

import org.elasticsearch.hadoop.HdpBootstrap;
import org.elasticsearch.hadoop.fixtures.KDCFixture;
import org.elasticsearch.hadoop.security.UgiUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.ChainedExternalResource;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({ AbstractSpnegoNegotiatorTest.class, AbstractSpnegoAuthSchemeTest.class, AbstractHadoopUserKerberosTest.class })
public class KerberosSuite {

    public static String PRINCIPAL_CLIENT = "client";
    public static String PRINCIPAL_SERVER = "server";
    public static String PRINCIPAL_HTTP = "HTTP/es.build.elastic.co";
    public static String DEFAULT_REALM = "BUILD.ELASTIC.CO";

    private static TemporaryFolder temporaryFolder = new TemporaryFolder();
    private static KDCFixture kdcFixture = new KDCFixture(temporaryFolder);

    @ClassRule
    public static ExternalResource resource = new ChainedExternalResource(temporaryFolder, kdcFixture);

    private static File KEYTAB_FILE;

    @BeforeClass
    public static void provision() throws Exception {
        HdpBootstrap.hackHadoopStagingOnWin();

        KEYTAB_FILE = temporaryFolder.newFile("test.keytab");
        kdcFixture.createPrincipal(KEYTAB_FILE, PRINCIPAL_CLIENT, PRINCIPAL_SERVER, PRINCIPAL_HTTP);
    }

    @AfterClass
    public static void resetUGI() {
        UgiUtil.resetUGI();
    }

    public static File getKeytabFile() {
        return KEYTAB_FILE;
    }

}
