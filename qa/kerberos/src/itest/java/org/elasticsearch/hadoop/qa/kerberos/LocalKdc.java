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

package org.elasticsearch.hadoop.qa.kerberos;

import java.io.File;

import org.junit.rules.ExternalResource;

public class LocalKdc extends ExternalResource {

    /**
     * When set true in System properties, we should try seeing if the KDC is up and we're just missing the KRB5.CONF file.
     * Useful for running the test suite in an IDE.
     */
    private static final String AUTO_SENSE_KDC = "test.kdc.autosense";

    private static final String KRB_CONF = "java.security.krb5.conf";

    private boolean sensedInstall = false;

    @Override
    protected void before() throws Throwable {
        boolean autoSense = Boolean.parseBoolean(System.getProperty(AUTO_SENSE_KDC, "false"));

        // Check krb5.conf file
        if (System.getProperty(KRB_CONF) == null) {
            // Tests that use this need to have a local kdc running. Check to see if there's a fixture near by.
            File fixtureDir = new File("build/fixtures/kdcFixture");
            if (autoSense && new File(fixtureDir, "pid").exists()) {
                // Is KDC running but we don't have the setting?
                sensedInstall = true;
                File confFile = new File(fixtureDir, "krb5.conf");
                System.setProperty(KRB_CONF, confFile.getAbsolutePath());
                System.out.printf("Automatically sensed running KDC and configuration located at [%s].%n", confFile.getAbsolutePath());
            } else {
                throw new IllegalStateException("Cannot run test that requires local kerberos instance running");
            }
        }
        System.out.printf("Using KDC Configuration [%s].%n", System.getProperty(KRB_CONF));
    }

    @Override
    protected void after() {
        if (sensedInstall) {
            // If we found the fixture on our own, reset the kerberos settings to the way they were.
            System.clearProperty(KRB_CONF);
            sensedInstall = false;
        }
    }
}
