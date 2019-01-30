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

import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;

import org.elasticsearch.hadoop.EsHadoopException;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.util.ClusterName;

public class JdkUser implements User {

    /**
     * Simplify getting and setting of tokens on a Subject by letting us store and retrieve them by name
     */
    static class EsTokenHolder {
        private Map<String, EsToken> creds = new HashMap<String, EsToken>();

        EsToken getCred(String alias) {
            return creds.get(alias);
        }

        Collection<EsToken> getCreds() {
            return creds.values();
        }

        void setCred(String alias, EsToken cred) {
            creds.put(alias, cred);
        }
    }

    private final Subject subject;
    private final Settings providerSettings;

    public JdkUser(Subject subject, Settings providerSettings) {
        this.subject = subject;
        this.providerSettings = providerSettings;
    }

    @Override
    public <T> T doAs(PrivilegedAction<T> action) {
        return Subject.doAs(subject, action);
    }

    @Override
    public <T> T doAs(PrivilegedExceptionAction<T> action) throws EsHadoopException {
        try {
            return Subject.doAs(subject, action);
        } catch (PrivilegedActionException e) {
            if (e.getCause() instanceof EsHadoopException) {
                throw ((EsHadoopException) e.getCause());
            } else {
                throw new EsHadoopException(e.getCause());
            }
        }
    }

    @Override
    public EsToken getEsToken(String clusterName) {
        // An unset cluster name - Wouldn't have a token for it.
        if (clusterName == null || clusterName.equals("") || clusterName.equals(ClusterName.UNNAMED_CLUSTER_NAME)) {
            return null;
        }
        Set<EsTokenHolder> credSet = subject.getPrivateCredentials(EsTokenHolder.class);
        if (credSet.isEmpty()) {
            return null;
        } else {
            EsTokenHolder holder = credSet.iterator().next();
            return holder.getCred(clusterName);
        }
    }

    @Override
    public Iterable<EsToken> getAllEsTokens() {
        Set<EsTokenHolder> credSet = subject.getPrivateCredentials(EsTokenHolder.class);
        if (credSet.isEmpty()) {
            return Collections.emptyList();
        } else {
            EsTokenHolder holder = credSet.iterator().next();
            List<EsToken> tokens = new ArrayList<>();
            tokens.addAll(holder.getCreds());
            return Collections.unmodifiableList(tokens);
        }
    }

    @Override
    public void addEsToken(EsToken esToken) {
        Iterator<EsTokenHolder> credSet = subject.getPrivateCredentials(EsTokenHolder.class).iterator();
        EsTokenHolder creds = null;
        if (credSet.hasNext()) {
            creds = credSet.next();
        } else {
            creds = new EsTokenHolder();
            subject.getPrivateCredentials().add(creds);
        }
        creds.setCred(esToken.getClusterName(), esToken);
    }

    @Override
    public String getUserName() {
        KerberosPrincipal principal = getKerberosPrincipal();
        if (principal == null) {
            return null;
        }
        return principal.getName();
    }

    @Override
    public KerberosPrincipal getKerberosPrincipal() {
        Iterator<KerberosPrincipal> iter = subject.getPrincipals(KerberosPrincipal.class).iterator();
        if (iter.hasNext()) {
            return iter.next();
        }
        return null;
    }

    @Override
    public boolean isProxyUser() {
        // Proxy users are a strictly Hadoop based mechanism.
        // A basic Subject will never be a Proxy user on its own.
        return false;
    }

    @Override
    public UserProvider getRealUserProvider() {
        // Since there is no real user under this subject, just return a
        // new provider with the same settings as this user's provider.
        UserProvider sameProvider = new JdkUserProvider();
        sameProvider.setSettings(providerSettings);
        return sameProvider;
    }

    @Override
    public String toString() {
        return "JdkUser{" +
                "subject=" + subject +
                '}';
    }
}
