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

package org.elasticsearch.hadoop.mr.security;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;

import javax.security.auth.kerberos.KerberosPrincipal;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.elasticsearch.hadoop.EsHadoopException;
import org.elasticsearch.hadoop.security.EsToken;
import org.elasticsearch.hadoop.security.User;
import org.elasticsearch.hadoop.serialization.EsHadoopSerializationException;
import org.elasticsearch.hadoop.util.ClusterName;

/**
 * Provides access to user operations from Hadoop's UserGroupInformation class.
 */
public class HadoopUser implements User {

    private final UserGroupInformation ugi;

    public HadoopUser(UserGroupInformation ugi) {
        this.ugi = ugi;
    }

    @Override
    public <T> T doAs(PrivilegedAction<T> action) {
        return ugi.doAs(action);
    }

    @Override
    public <T> T doAs(PrivilegedExceptionAction<T> action) throws EsHadoopException {
        try {
            return ugi.doAs(action);
        } catch (IOException e) {
            throw new EsHadoopException(e);
        } catch (InterruptedException e) {
            throw new EsHadoopException(e);
        } catch (UndeclaredThrowableException e) {
            throw new EsHadoopException(e);
        }
    }

    @Override
    public EsToken getEsToken(String clusterName) {
        // An unset cluster name - Wouldn't have a token for it.
        if (clusterName == null || clusterName.equals("") || clusterName.equals(ClusterName.UNNAMED_CLUSTER_NAME)) {
            return null;
        }
        for (Token<? extends TokenIdentifier> token : ugi.getTokens()) {
            if (EsTokenIdentifier.KIND_NAME.equals(token.getKind()) && clusterName.equals(token.getService().toString())) {
                try {
                    return new EsToken(new DataInputStream(new ByteArrayInputStream(token.getPassword())));
                } catch (IOException e) {
                    throw new EsHadoopSerializationException("Could not read token information from UGI", e);
                }
            }
        }
        return null; // Token not found
    }

    @Override
    public void addEsToken(EsToken esToken) {
        EsTokenIdentifier identifier = new EsTokenIdentifier();
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        try {
            esToken.writeOut(new DataOutputStream(buffer));
        } catch (IOException e) {
            throw new EsHadoopException("Could not serialize token information", e);
        }
        byte[] id = identifier.getBytes();
        byte[] pw = buffer.toByteArray();
        Text kind = identifier.getKind();
        Text service = new Text(esToken.getClusterName());
        Token<EsTokenIdentifier> token = new Token<EsTokenIdentifier>(id, pw, kind, service);
        ugi.addToken(token);
    }

    @Override
    public KerberosPrincipal getKerberosPrincipal() {
        if (ugi.hasKerberosCredentials()) {
            return new KerberosPrincipal(ugi.getUserName());
        }
        return null;
    }
}
