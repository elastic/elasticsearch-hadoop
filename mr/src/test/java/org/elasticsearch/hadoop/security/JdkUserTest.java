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

package org.elasticsearch.hadoop.security;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;

import org.elasticsearch.hadoop.util.ClusterName;
import org.elasticsearch.hadoop.util.EsMajorVersion;
import org.elasticsearch.hadoop.util.TestSettings;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

public class JdkUserTest {

    @Test
    public void getEsToken() {
        Subject subject = new Subject();

        String testClusterName = "testClusterName";

        User jdkUser = new JdkUser(subject, new TestSettings());
        assertThat(jdkUser.getEsToken(null), is(nullValue()));
        assertThat(jdkUser.getEsToken(""), is(nullValue()));
        assertThat(jdkUser.getEsToken(ClusterName.UNNAMED_CLUSTER_NAME), is(nullValue()));
        assertThat(jdkUser.getEsToken(testClusterName), is(nullValue()));

        EsToken testToken = new EsToken("gmarx", "swordfish", "mary", System.currentTimeMillis() + 100000L, testClusterName, EsMajorVersion.LATEST);
        EsToken unnamedToken = new EsToken("luggage", "12345", "12345", System.currentTimeMillis() + 100000L, ClusterName.UNNAMED_CLUSTER_NAME, EsMajorVersion.LATEST);

        JdkUser.EsTokenHolder holder = new JdkUser.EsTokenHolder();
        holder.setCred(testClusterName, testToken);
        holder.setCred(ClusterName.UNNAMED_CLUSTER_NAME, unnamedToken);
        subject.getPrivateCredentials().add(holder);

        assertThat(jdkUser.getEsToken(null), is(nullValue()));
        assertThat(jdkUser.getEsToken(""), is(nullValue()));
        assertThat(jdkUser.getEsToken(ClusterName.UNNAMED_CLUSTER_NAME), is(nullValue()));
        assertThat(jdkUser.getEsToken(testClusterName), is(equalTo(testToken)));

        // Using getPrivateCredentials() always returns the creds in FIFO order, but using getPrivateCredentials(Class<T>) returns in
        // random order. Cannot guarantee which creds are actually first added, and probably shouldn't try to determine it at runtime.
//        // Add erroneous overlapping secret holder
//        JdkUser.EsTokenHolder holder2 = new JdkUser.EsTokenHolder();
//        holder2.setCred(testClusterName, unnamedToken);
//        subject.getPrivateCredentials().add(holder2);
//
//        // Should only return contents of first holder
//        assertThat(jdkUser.getEsToken(null), is(nullValue()));
//        assertThat(jdkUser.getEsToken(""), is(nullValue()));
//        assertThat(jdkUser.getEsToken(ClusterName.UNNAMED_CLUSTER_NAME), is(nullValue()));
//        assertThat(jdkUser.getEsToken(testClusterName), is(equalTo(testToken)));
    }

    @Test
    public void addEsToken() {
        String testClusterName = "testClusterName";

        User jdkUser = new JdkUser(new Subject(), new TestSettings());
        assertThat(jdkUser.getEsToken(null), is(nullValue()));
        assertThat(jdkUser.getEsToken(""), is(nullValue()));
        assertThat(jdkUser.getEsToken(ClusterName.UNNAMED_CLUSTER_NAME), is(nullValue()));
        assertThat(jdkUser.getEsToken(testClusterName), is(nullValue()));

        EsToken testToken = new EsToken("gmarx", "swordfish", "mary", System.currentTimeMillis() + 100000L, testClusterName, EsMajorVersion.LATEST);
        EsToken testToken2 = new EsToken("zmarx", "pantomime", "pantomime", System.currentTimeMillis() + 100000L, testClusterName, EsMajorVersion.LATEST);
        EsToken unnamedToken = new EsToken("luggage", "12345", "12345", System.currentTimeMillis() + 100000L, ClusterName.UNNAMED_CLUSTER_NAME, EsMajorVersion.LATEST);

        jdkUser.addEsToken(testToken);
        jdkUser.addEsToken(unnamedToken);

        assertThat(jdkUser.getEsToken(null), is(nullValue()));
        assertThat(jdkUser.getEsToken(""), is(nullValue()));
        assertThat(jdkUser.getEsToken(ClusterName.UNNAMED_CLUSTER_NAME), is(nullValue()));
        assertThat(jdkUser.getEsToken(testClusterName), is(equalTo(testToken)));

        jdkUser.addEsToken(testToken2);

        assertThat(jdkUser.getEsToken(null), is(nullValue()));
        assertThat(jdkUser.getEsToken(""), is(nullValue()));
        assertThat(jdkUser.getEsToken(ClusterName.UNNAMED_CLUSTER_NAME), is(nullValue()));
        assertThat(jdkUser.getEsToken(testClusterName), is(equalTo(testToken2)));
    }

    @Test
    public void getKerberosPrincipal() {
        Subject subject = new Subject();
        User jdkUser = new JdkUser(subject, new TestSettings());

        assertThat(jdkUser.getKerberosPrincipal(), is(nullValue()));

        KerberosPrincipal principal = new KerberosPrincipal("username@BUILD.ELASTIC.CO");
        subject.getPrincipals().add(principal);

        assertThat(jdkUser.getKerberosPrincipal(), is(equalTo(principal)));
    }
}