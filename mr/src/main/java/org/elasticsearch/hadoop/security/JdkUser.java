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
import java.util.Set;

import javax.security.auth.Subject;

import org.elasticsearch.hadoop.EsHadoopException;

public class JdkUser implements User {

    private final Subject subject;

    public JdkUser(Subject subject) {
        this.subject = subject;
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
    public EsToken getEsToken() {
        Set<EsToken> creds = subject.getPrivateCredentials(EsToken.class);
        if (creds.isEmpty()) {
            return null;
        } else {
            return creds.iterator().next();
        }
    }

    @Override
    public void setEsToken(EsToken esToken) {
        Set<EsToken> creds = subject.getPrivateCredentials(EsToken.class);
        if (!creds.isEmpty()) {
            for (EsToken cred : creds) {
                subject.getPrivateCredentials().remove(cred);
            }
        }
        subject.getPrivateCredentials().add(esToken);
    }
}
