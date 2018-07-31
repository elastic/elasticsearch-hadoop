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

import java.io.File;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedExceptionAction;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.elasticsearch.hadoop.EsHadoopIllegalStateException;
import org.elasticsearch.hadoop.HdpBootstrap;
import org.elasticsearch.hadoop.fixtures.KDCFixture;
import org.elasticsearch.hadoop.rest.EsHadoopTransportException;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.ChainedExternalResource;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public class SpnegoNegotiatorTest {

    private static TemporaryFolder temporaryFolder = new TemporaryFolder();
    private static KDCFixture kdcFixture = new KDCFixture(temporaryFolder);

    @ClassRule
    public static ExternalResource resource = new ChainedExternalResource(temporaryFolder, kdcFixture);

    private static File KEYTAB_FILE;

    @BeforeClass
    public static void provision() throws Exception {
        HdpBootstrap.hackHadoopStagingOnWin();

        KEYTAB_FILE = temporaryFolder.newFile("test.keytab");
        kdcFixture.createPrincipal(KEYTAB_FILE, "client", "server");
    }

    @Test(expected = EsHadoopIllegalStateException.class)
    public void testPreemptNegotiatorWithChallengeFails() throws IOException, InterruptedException {
        // Configure logins
        Configuration configuration = new Configuration();
        SecurityUtil.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS, configuration);
        UserGroupInformation.setConfiguration(configuration);

        // Login as Client and Create negotiator
        UserGroupInformation client = UserGroupInformation.loginUserFromKeytabAndReturnUGI("client", KEYTAB_FILE.getAbsolutePath());
        final SpnegoNegotiator spnegoNegotiator = client.doAs(new PrivilegedExceptionAction<SpnegoNegotiator>() {
            @Override
            public SpnegoNegotiator run() throws Exception {
                return new SpnegoNegotiator("client", "server");
            }
        });

        byte[] token = new byte[]{1,2,3,4,5};
        spnegoNegotiator.setTokenData(Base64.encodeBase64String(token));
        fail("Negotiator should break if it is given a challenge before it initiates the negotiations");
    }

    @Test(expected = UndeclaredThrowableException.class)
    public void testFalseResponseFromServerFails() throws IOException, InterruptedException {
        // Configure logins
        Configuration configuration = new Configuration();
        SecurityUtil.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS, configuration);
        UserGroupInformation.setConfiguration(configuration);

        // Login as Client and Create negotiator
        UserGroupInformation client = UserGroupInformation.loginUserFromKeytabAndReturnUGI("client", KEYTAB_FILE.getAbsolutePath());
        final SpnegoNegotiator spnegoNegotiator = client.doAs(new PrivilegedExceptionAction<SpnegoNegotiator>() {
            @Override
            public SpnegoNegotiator run() throws Exception {
                return new SpnegoNegotiator("client", "server");
            }
        });

        String baseToken = client.doAs(new PrivilegedExceptionAction<String>() {
            @Override
            public String run() throws Exception {
                return spnegoNegotiator.send();
            }
        });
        final byte[] token = Base64.decodeBase64(baseToken);

        spnegoNegotiator.setTokenData(Base64.encodeBase64String(new byte[]{1,2,3,4,5}));
        client.doAs(new PrivilegedExceptionAction<String>() {
            @Override
            public String run() throws Exception {
                return spnegoNegotiator.send();
            }
        });
        fail("Defective token given to Negotiator and accepted.");
    }

    @Test(expected = EsHadoopTransportException.class)
    public void testMissingNegotiationTokenFails() throws IOException, GSSException, InterruptedException {
        // Configure logins
        Configuration configuration = new Configuration();
        SecurityUtil.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS, configuration);
        UserGroupInformation.setConfiguration(configuration);

        // Login as Client and Create negotiator
        UserGroupInformation client = UserGroupInformation.loginUserFromKeytabAndReturnUGI("client", KEYTAB_FILE.getAbsolutePath());
        final SpnegoNegotiator spnegoNegotiator = client.doAs(new PrivilegedExceptionAction<SpnegoNegotiator>() {
            @Override
            public SpnegoNegotiator run() throws Exception {
                return new SpnegoNegotiator("client", "server");
            }
        });

        client.doAs(new PrivilegedExceptionAction<String>() {
            @Override
            public String run() throws Exception {
                return spnegoNegotiator.send();
            }
        });

        // No setting of response token
        client.doAs(new PrivilegedExceptionAction<String>() {
            @Override
            public String run() throws Exception {
                return spnegoNegotiator.send();
            }
        });
        fail("No token given to Negotiator but was accepted anyway.");
    }

    @Test
    public void testSuccessfulNegotiate() throws IOException, GSSException, InterruptedException {
        // Mechanisms
        final GSSManager gssManager = GSSManager.getInstance();
        final Oid spnegoOid = new Oid("1.3.6.1.5.5.2");

        // Configure logins
        Configuration configuration = new Configuration();
        SecurityUtil.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS, configuration);
        UserGroupInformation.setConfiguration(configuration);

        // Login as Server
        UserGroupInformation server = UserGroupInformation.loginUserFromKeytabAndReturnUGI("server", KEYTAB_FILE.getAbsolutePath());
        final GSSName gssServicePrincipalName = gssManager.createName("server", GSSName.NT_USER_NAME);
        final GSSCredential gssServiceCredential = server.doAs(new PrivilegedExceptionAction<GSSCredential>() {
            @Override
            public GSSCredential run() throws Exception {
                return gssManager.createCredential(
                        gssServicePrincipalName,
                        GSSCredential.DEFAULT_LIFETIME,
                        spnegoOid,
                        GSSCredential.ACCEPT_ONLY
                );
            }
        });
        final GSSContext serverCtx = gssManager.createContext(gssServiceCredential);

        // Login as Client and Create negotiator
        UserGroupInformation client = UserGroupInformation.loginUserFromKeytabAndReturnUGI("client", KEYTAB_FILE.getAbsolutePath());
        final SpnegoNegotiator spnegoNegotiator = client.doAs(new PrivilegedExceptionAction<SpnegoNegotiator>() {
            @Override
            public SpnegoNegotiator run() throws Exception {
                return new SpnegoNegotiator("client", "server");
            }
        });

        byte[] token = new byte[0];
        boolean authenticated = false;

        for (int idx = 0; idx < 100; idx++) {
            if (!spnegoNegotiator.established()) {
                if (token.length > 0) {
                    spnegoNegotiator.setTokenData(Base64.encodeBase64String(token));
                }
                String baseToken = client.doAs(new PrivilegedExceptionAction<String>() {
                    @Override
                    public String run() throws Exception {
                        return spnegoNegotiator.send();
                    }
                });
                token = Base64.decodeBase64(baseToken);
            }

            if (!spnegoNegotiator.established() && serverCtx.isEstablished()) {
                fail("Server is established, but client is not.");
            }

            if (!serverCtx.isEstablished()) {
                final byte[] currentToken = token;
                token = server.doAs(new PrivilegedExceptionAction<byte[]>() {
                    @Override
                    public byte[] run() throws Exception {
                        return serverCtx.acceptSecContext(currentToken, 0, currentToken.length);
                    }
                });
            }

            if (serverCtx.isEstablished() && spnegoNegotiator.established()) {
                authenticated = true;
                break;
            }
        }

        assertThat(authenticated, is(true));
        assertThat(serverCtx.isEstablished(), is(true));
        assertThat(spnegoNegotiator.established(), is(true));
    }
}