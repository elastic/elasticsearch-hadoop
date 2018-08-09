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
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.auth.AuthScheme;
import org.apache.commons.httpclient.auth.AuthenticationException;
import org.apache.commons.httpclient.auth.MalformedChallengeException;
import org.elasticsearch.hadoop.rest.commonshttp.auth.EsHadoopAuthPolicies;
import org.elasticsearch.hadoop.util.StringUtils;

public class TokenAuthScheme implements AuthScheme {

    private boolean complete = false;

    @Override
    public boolean isConnectionBased() {
        // Token is sent every request
        return false;
    }

    /**
     * Used to look up the parsed challenges from a request that has returned a 401.
     *
     * @return The scheme name as it appears in the WWW-Authenticate header challenge
     */
    @Override
    public String getSchemeName() {
        return EsHadoopAuthPolicies.BEARER;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Called using the challenge text parsed from the header that is associated with this scheme's name.
     * </p>
     */
    @Override
    public void processChallenge(String challenge) throws MalformedChallengeException {
        // Do nothing? If the token can be accepted, we'll try sending it.
    }

    /**
     * Implementation method for authentication
     */
    private String authenticate(Credentials credentials) throws AuthenticationException {
        if (!(credentials instanceof EsTokenCredentials)) {
            throw new AuthenticationException("Incorrect credentials type provided. Expected [" + EsTokenCredentials.class.getName()
                    + "] but got [" + credentials.getClass().getName() + "]");
        }

        EsTokenCredentials esTokenCredentials = ((EsTokenCredentials) credentials);
        String authString = null;

        if (esTokenCredentials.getToken() != null && StringUtils.hasText(esTokenCredentials.getToken().getAccessToken())) {
            authString = EsHadoopAuthPolicies.BEARER + " " + esTokenCredentials.getToken().getAccessToken();
            complete = true;
        }

        return authString;
    }

    /**
     * Returns the text to send via the Authenticate header on the next request.
     */
    @Override
    public String authenticate(Credentials credentials, HttpMethod method) throws AuthenticationException {
        return authenticate(credentials);
    }

    /**
     * Deprecated method, can still be authenticated with credentials.
     */
    @Override
    public String authenticate(Credentials credentials, String method, String uri) throws AuthenticationException {
        return authenticate(credentials);
    }

    @Override
    public boolean isComplete() {
        return complete;
    }

    @Override
    public String getRealm() {
        // It's not clear what to return here. It seems that null works fine for functional use as it means "Any Realm"
        return null;
    }

    @Override
    public String getParameter(String name) {
        // We don't need no stinkin' parameters
        return null;
    }

    @Override
    public String getID() {
        // Return Scheme Name for maximum bwc safety.
        return getSchemeName();
    }
}
