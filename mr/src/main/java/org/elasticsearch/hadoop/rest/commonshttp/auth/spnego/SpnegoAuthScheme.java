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

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;

import org.apache.commons.httpclient.Credentials;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.URIException;
import org.apache.commons.httpclient.auth.AuthScheme;
import org.apache.commons.httpclient.auth.AuthenticationException;
import org.apache.commons.httpclient.auth.MalformedChallengeException;
import org.elasticsearch.hadoop.rest.commonshttp.auth.EsHadoopAuthPolicies;
import org.elasticsearch.hadoop.util.StringUtils;
import org.ietf.jgss.GSSException;

public class SpnegoAuthScheme implements AuthScheme {

    private static final String HOSTNAME_PATTERN = "_HOST";
    private String challenge;
    private SpnegoNegotiator spnegoNegotiator;

    @Override
    public boolean isConnectionBased() {
        // SPNEGO is request based
        return false;
    }

    /**
     * Used to look up the parsed challenges from a request that has returned a 401.
     *
     * @return The scheme name as it appears in the WWW-Authenticate header challenge
     */
    @Override
    public String getSchemeName() {
        return EsHadoopAuthPolicies.NEGOTIATE;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Called using the challenge text parsed from the header that is associated with this scheme's name.
     * May advance the authentication process across multiple calls.
     * </p>
     */
    @Override
    public void processChallenge(String challenge) throws MalformedChallengeException {
        // Parse Challenge Data
        // Challenge is base64 string to be given to gss context
        if (StringUtils.hasText(challenge)) {
            // Remove leading auth scheme name and trim data
            this.challenge = challenge.substring(EsHadoopAuthPolicies.NEGOTIATE.length()).trim();
        }
    }

    /**
     * Implementation method that returns the text to send via the Authenticate header on the next request.
     */
    private String authenticate(Credentials credentials, URI requestURI) throws AuthenticationException {
        if (!(credentials instanceof SpnegoCredentials)) {
            throw new AuthenticationException("Invalid credentials type provided to " + this.getClass().getName() + "." +
                    "Expected " + SpnegoCredentials.class.getName() + " but got " + credentials.getClass().getName());
        }

        final SpnegoCredentials spnegoCredentials = (SpnegoCredentials) credentials;

        try {
            // Initialize negotiator
            if (spnegoNegotiator == null) {
                // Determine host principal
                String servicePrincipal = spnegoCredentials.getServicePrincipalName();
                if (spnegoCredentials.getServicePrincipalName().contains(HOSTNAME_PATTERN)) {
                    String fqdn = getFQDN(requestURI);
                    String[] components = spnegoCredentials.getServicePrincipalName().split("[/@]");
                    if (components.length != 3 || !components[1].equals(HOSTNAME_PATTERN)) {
                        throw new AuthenticationException("Malformed service principal name [" + spnegoCredentials.getServicePrincipalName()
                                + "]. To use host substitution, the principal must be of the format [serviceName/_HOST@REALM.NAME].");
                    }
                    servicePrincipal = components[0] + "/" + fqdn.toLowerCase() + "@" + components[2];
                }
                spnegoNegotiator = new SpnegoNegotiator(spnegoCredentials.getPrincipalName(), servicePrincipal);
            }

            // Perform GSS Dance
            String authString;
            if (StringUtils.hasText(challenge)) {
                authString = spnegoNegotiator.send(challenge);
            } else {
                authString = spnegoNegotiator.send();
            }
            this.challenge = null;

            // Prepend the authentication scheme to use
            if (authString != null) {
                authString = EsHadoopAuthPolicies.NEGOTIATE + " " + authString;
            }
            return authString;
        } catch (GSSException e) {
            throw new AuthenticationException("Could not authenticate", e);
        } catch (UnknownHostException e) {
            throw new AuthenticationException("Could not authenticate", e);
        }
    }

    protected String getFQDN(URI requestURI) throws UnknownHostException {
        String host = requestURI.getHost();
        InetAddress address = InetAddress.getByName(host);
        return address.getCanonicalHostName();
    }

    /**
     * Returns the text to send via the Authenticate header on the next request.
     */
    @Override
    public String authenticate(Credentials credentials, HttpMethod method) throws AuthenticationException {
        try {
            return authenticate(credentials, URI.create(method.getURI().getURI()));
        } catch (URIException e) {
            throw new AuthenticationException("Could not determine request URI", e);
        }
    }

    /**
     * Deprecated method, can still be authenticated with credentials.
     */
    @Override
    public String authenticate(Credentials credentials, String method, String uri) throws AuthenticationException {
        return authenticate(credentials, URI.create(uri));
    }

    @Override
    public boolean isComplete() {
        return spnegoNegotiator.established();
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
        // Return the Scheme Name for maximum bwc safety
        return getSchemeName();
    }
}
