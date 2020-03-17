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
import java.lang.reflect.Field;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.elasticsearch.hadoop.security.UgiUtil;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

public class KDCFixture extends ExternalResource {

    private TemporaryFolder temporaryFolder;
    private MiniKdc kdc;
    private String previousDefaultRealm;

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

        /*
         * So, this test suite is run alongside other suites that are initializing static state
         * all throughout the Hadoop code with the assumption that Kerberos doesn't exist, and
         * no one in this JVM will ever care about it existing. KerberosName has a static field
         * set once and left as-is at class loading time. That field contains the default realm
         * as specified by the JVM's krb5 conf file. MiniKdc adds a test conf file to the JVM
         * properties after it starts up. We need to smash the glass and update the defaultRealm
         * field on the KerberosName class or else Hadoop will not be able to map a Kerberos
         * Principal Name to a regular user name with the DEFAULT rule.
         */
        Field defaultRealm = KerberosName.class.getDeclaredField("defaultRealm");
        defaultRealm.setAccessible(true);
        previousDefaultRealm = (String) defaultRealm.get(null);
        defaultRealm.set(null, KerberosUtil.getDefaultRealm());
    }

    public void createPrincipal(String principal, String password) throws Exception {
        kdc.createPrincipal(principal, password);
    }

    public void createPrincipal(File keytab, List<String> principals) throws Exception {
        kdc.createPrincipal(keytab, principals.toArray(new String[0]));
    }

    public void createPrincipal(File keytab, String... principals) throws Exception {
        kdc.createPrincipal(keytab, principals);
    }

    @Override
    protected void after() {
        kdc.stop();
        /*
         * Replace the default realm information on the KerberosName class so that we don't interfere
         * with any other test suites in the future.
         */
        try {
            Field defaultRealm = KerberosName.class.getDeclaredField("defaultRealm");
            defaultRealm.setAccessible(true);
            defaultRealm.set(null, previousDefaultRealm);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Couldn't access defaultRealm field on KerberosName", e);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException("Couldn't find defaultRealm field on KerberosName", e);
        }
        UgiUtil.resetUGI();
    }
}
