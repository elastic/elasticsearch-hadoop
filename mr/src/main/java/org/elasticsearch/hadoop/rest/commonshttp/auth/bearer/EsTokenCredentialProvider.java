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

package org.elasticsearch.hadoop.rest.commonshttp.auth.bearer;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import org.apache.commons.httpclient.Credentials;
import org.apache.commons.httpclient.auth.AuthScheme;
import org.apache.commons.httpclient.auth.CredentialsNotAvailableException;
import org.apache.commons.httpclient.auth.CredentialsProvider;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.elasticsearch.hadoop.rest.commonshttp.auth.EsHadoopAuthPolicies;
import org.elasticsearch.hadoop.mr.security.EsTokenIdentifier;
import org.elasticsearch.hadoop.security.EsToken;

public class EsTokenCredentialProvider implements CredentialsProvider {

    private UserGroupInformation userGroupInformation;

    public EsTokenCredentialProvider(UserGroupInformation userGroupInformation) {
        this.userGroupInformation = userGroupInformation;
    }

    @Override
    public Credentials getCredentials(AuthScheme scheme, String host, int port, boolean proxy) throws CredentialsNotAvailableException {
        if (!scheme.getSchemeName().equals(EsHadoopAuthPolicies.BEARER)) {
            throw new CredentialsNotAvailableException("Could not provide credentials for incorrect auth scheme ["+scheme.getSchemeName()+"]");
        }

        Token<? extends TokenIdentifier> esToken = null;
        for (Token<? extends TokenIdentifier> token : userGroupInformation.getTokens()) {
            // TODO: If we ever support interacting with different ES clusters, this should be changed to select the appropriate token
            if (EsTokenIdentifier.KIND_NAME.equals(token.getKind())) {
                esToken = token;
                break;
            }
        }

        if (esToken == null) {
            throw new CredentialsNotAvailableException("Could not locate valid Elasticsearch token in subject credentials");
        }

        try {
            return new EsTokenCredentials(new EsToken(new DataInputStream(new ByteArrayInputStream(esToken.getPassword()))));
        } catch (IOException e) {
            throw new CredentialsNotAvailableException("Could not decode token data", e);
        }
    }
}
