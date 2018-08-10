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

import org.apache.commons.httpclient.Credentials;
import org.apache.commons.httpclient.auth.AuthScheme;
import org.apache.commons.httpclient.auth.CredentialsNotAvailableException;
import org.apache.commons.httpclient.auth.CredentialsProvider;
import org.elasticsearch.hadoop.rest.commonshttp.auth.EsHadoopAuthPolicies;
import org.elasticsearch.hadoop.security.EsToken;
import org.elasticsearch.hadoop.security.UserProvider;

public class EsTokenCredentialProvider implements CredentialsProvider {

    private UserProvider userProvider;

    public EsTokenCredentialProvider(UserProvider provider) {
        this.userProvider = provider;
    }

    @Override
    public Credentials getCredentials(AuthScheme scheme, String host, int port, boolean proxy) throws CredentialsNotAvailableException {
        if (!scheme.getSchemeName().equals(EsHadoopAuthPolicies.BEARER)) {
            throw new CredentialsNotAvailableException("Could not provide credentials for incorrect auth scheme ["+scheme.getSchemeName()+"]");
        }
        EsToken esToken = userProvider.getUser().getEsToken();
        if (esToken == null) {
            throw new CredentialsNotAvailableException("Could not locate valid Elasticsearch token in subject credentials");
        }
        return new EsTokenCredentials(esToken);
    }
}
