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

import java.io.Closeable;
import java.io.IOException;

import org.apache.commons.codec.binary.Base64;
import org.elasticsearch.hadoop.EsHadoopIllegalStateException;
import org.elasticsearch.hadoop.rest.EsHadoopTransportException;
import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.StringUtils;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;

/**
 * Contains the state management and logic for parsing, validating, and constructing messages
 * to be used in SPNEGO authentication using Java's GSS API. Must be used while running-as the
 * currently logged in user subject from which to pull credentials.
 *
 * Yes, the name is redundant.
 */
public class SpnegoNegotiator implements Closeable {

    private static final String SPNEGO_OID = "1.3.6.1.5.5.2";

    private final GSSManager gssManager;
    private final Oid spnegoOID;
    private final GSSName servicePrincipalName;
    private final GSSCredential userCredential;

    private GSSContext gssContext = null;
    private byte[] token = null;

    public SpnegoNegotiator(String userPrincipal, String serverPrincipal) throws GSSException {
        this.gssManager = GSSManager.getInstance();
        this.spnegoOID = new Oid(SPNEGO_OID);

        GSSName userPrincipalName = this.gssManager.createName(userPrincipal, GSSName.NT_USER_NAME, this.spnegoOID);
        this.servicePrincipalName = this.gssManager.createName(serverPrincipal, GSSName.NT_USER_NAME);

        this.userCredential = this.gssManager.createCredential(
                userPrincipalName,
                GSSCredential.DEFAULT_LIFETIME,
                this.spnegoOID,
                GSSCredential.INITIATE_ONLY
        );
    }

    public void setTokenData(String data) {
        if (gssContext == null) {
            throw new EsHadoopIllegalStateException("GSS Context not yet initialized. Client must be the initiator.");
        }
        token = Base64.decodeBase64(data);
    }

    public String send(String data) throws GSSException {
        setTokenData(data);
        return send();
    }

    public String send() throws GSSException {
        byte[] sendData;
        if (gssContext == null) {
            Assert.isTrue(token == null, "GSS Context not yet initialized. Client must be the initiator.");
            gssContext = gssManager.createContext(servicePrincipalName, spnegoOID, userCredential, GSSCredential.DEFAULT_LIFETIME);
            sendData = gssContext.initSecContext(new byte[0], 0, 0);
        } else if (token != null) {
            sendData = gssContext.initSecContext(token, 0, token.length);
            token = null;
        } else {
            throw new EsHadoopTransportException("Missing required negotiation token");
        }

        if (sendData == null) {
            return null;
        } else {
            return new String(Base64.encodeBase64(sendData), StringUtils.UTF_8);
        }
    }

    public boolean established() {
        return gssContext != null && gssContext.isEstablished();
    }

    @Override
    public void close() throws IOException {
        if (gssContext != null) {
            try {
                gssContext.dispose();
            } catch (GSSException e) {
                throw new IOException("Could not dispose of GSSContext", e);
            }
        }
    }
}
