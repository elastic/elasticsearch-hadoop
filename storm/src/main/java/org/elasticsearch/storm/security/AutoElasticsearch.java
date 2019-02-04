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

package org.elasticsearch.storm.security;

import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.storm.security.INimbusCredentialPlugin;
import org.apache.storm.security.auth.IAutoCredentials;
import org.apache.storm.security.auth.ICredentialsRenewer;
import org.elasticsearch.hadoop.EsHadoopException;
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.cfg.CompositeSettings;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.mr.security.TokenUtil;
import org.elasticsearch.hadoop.rest.InitializationUtils;
import org.elasticsearch.hadoop.rest.RestClient;
import org.elasticsearch.hadoop.security.AuthenticationMethod;
import org.elasticsearch.hadoop.security.EsToken;
import org.elasticsearch.hadoop.security.JdkUser;
import org.elasticsearch.hadoop.security.JdkUserProvider;
import org.elasticsearch.hadoop.security.LoginUtil;
import org.elasticsearch.hadoop.security.User;
import org.elasticsearch.hadoop.util.FastByteArrayInputStream;
import org.elasticsearch.hadoop.util.FastByteArrayOutputStream;
import org.elasticsearch.hadoop.util.StringUtils;
import org.elasticsearch.storm.cfg.StormSettings;

/**
 * A storm plugin that logs in with Kerberos, acquires an Elasticsearch API Token,
 * serializes it for transit, unpacks it on the storm worker side, and manages the
 * process of updating or acquiring new tokens when the token is expired.
 */
public class AutoElasticsearch implements IAutoCredentials, ICredentialsRenewer, INimbusCredentialPlugin {

    private static final Log LOG = LogFactory.getLog(AutoElasticsearch.class);

    private static final String ELASTICSEARCH_CREDENTIALS = "ELASTICSEARCH_CREDENTIALS";

    public static final String USER_PRINCIPAL = "es.storm.autocredentials.user.principal";
    public static final String USER_KEYTAB = "es.storm.autocredentials.user.keytab";

    private StormSettings clusterSettings;

    @Override
    public void prepare(Map stormClusterConfiguration) {
        // Captures just the cluster configuration, topology specific configuration
        // will be passed in via method parameters later as needed
        LOG.debug("Receiving cluster configuration");
        clusterSettings = new StormSettings(stormClusterConfiguration);
    }

    /*
     * INimbusCredentialPlugin
     *
     * Interface provides methods for generating authentication credentials on Nimbus to be distributed
     * to worker processes.
     *
     * Storm only calls these methods if this is set as an auto credential on a topology's configuration.
     */

    /**
     * {@inheritDoc}
     * @deprecated This is available for any storm cluster that operates against the older method of obtaining credentials
     */
    @Override
    public void populateCredentials(Map<String, String> credentials, Map topologyConfiguration) {
        populateCredentials(credentials, topologyConfiguration, null);
    }

    @Override
    public void populateCredentials(Map<String, String> credentials, Map<String, Object> topologyConfiguration, String topologyOwnerPrincipal) {
        LOG.debug("Populating credentials...");
        // These maps can contain any sort of object so we'll rely on StormSettings instead of assuming that they're strings.
        StormSettings topologyConf = new StormSettings(topologyConfiguration);
        final Settings topologyAndClusterSettings = new CompositeSettings(Arrays.asList(topologyConf, clusterSettings));

        if (!AuthenticationMethod.KERBEROS.equals(topologyAndClusterSettings.getSecurityAuthenticationMethod())) {
            throw new EsHadoopIllegalArgumentException("Configured Elasticsearch autocredential plugin but did not enable ES Kerberos [" +
                    ConfigurationOptions.ES_SECURITY_AUTHENTICATION + "]. Bailing out...");
        }

        String userPrincipal = topologyAndClusterSettings.getProperty(USER_PRINCIPAL);
        if (userPrincipal == null) {
            throw new EsHadoopIllegalArgumentException("Configured Elasticsearch autocredential plugin but did not provide [" +
                    USER_PRINCIPAL + "] setting. Bailing out...");
        }

        String userKeytab = topologyAndClusterSettings.getProperty(USER_KEYTAB);
        if (userKeytab == null) {
            throw new EsHadoopIllegalArgumentException("Configured Elasticsearch autocredential plugin but did not provide [" +
                    USER_KEYTAB + "] setting. Bailing out...");
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Performing login for [%s] using [%s]", userPrincipal, userKeytab));
        }
        LoginContext loginContext;
        try {
            loginContext = LoginUtil.keytabLogin(userPrincipal, userKeytab);
        } catch (LoginException e) {
            throw new EsHadoopException("Could not perform keytab login", e);
        }

        // Ensure that the user provider is configured
        InitializationUtils.setUserProviderIfNotSet(topologyAndClusterSettings, JdkUserProvider.class, LOG);

        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Obtaining token for [%s]", userPrincipal));
        }
        EsToken token;
        try {
            token = Subject.doAs(loginContext.getSubject(), new PrivilegedExceptionAction<EsToken>() {
                @Override
                public EsToken run() throws Exception {
                    RestClient client = new RestClient(topologyAndClusterSettings);
                    try {
                        return client.createNewApiToken(TokenUtil.KEY_NAME_PREFIX + UUID.randomUUID().toString());
                    } finally {
                        client.close();
                    }
                }
            });
        } catch (PrivilegedActionException e) {
            throw new EsHadoopException("Could not retrieve delegation token", e);
        } finally {
            try {
                loginContext.logout();
            } catch (LoginException e) {
                LOG.warn("Could not complete logout operation", e);
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Obtained token [%s] for principal [%s]", token.getName(), userPrincipal));
        }
        putCred(ELASTICSEARCH_CREDENTIALS, token, credentials);
    }

    @Override
    public void shutdown() {
        // no op since it's never guaranteed to be called
    }

    /*
     * ICredentialsRenewer
     *
     * Interface that is called on Nimbus to update or generate new credentials
     */

    /**
     * {@inheritDoc}
     * @deprecated This is available for any storm cluster that operates against the older method of credential renewal
     */
    @Override
    public void renew(Map<String, String> credentials, Map topologyConf) {
        renew(credentials, topologyConf, null);
    }

    @Override
    public void renew(Map<String, String> credentials, Map<String, Object> topologyConf, String topologyOwnerPrincipal) {
        // Just get new credentials from the initial populate credentials call since API keys are not renewable.
        LOG.debug("Checking for credential renewal");
        EsToken token = getCred(ELASTICSEARCH_CREDENTIALS, credentials);
        if (token != null) {
            LOG.debug("Checking token lifetime to see if refresh is required...");
            // Check the time to live on the token against when the next refresh is likely to occur
            long currentTime = System.currentTimeMillis();
            long expiration = token.getExpirationTime();

            if (currentTime > expiration) {
                // Token is already expired, just renew it.
                LOG.debug("ES Token expired. Renewing token...");
                populateCredentials(credentials, topologyConf, topologyOwnerPrincipal);
                return;
            }

            int renewalWindowSeconds = clusterSettings.getNimbusCredentialRenewersFrequencySeconds();

            if (renewalWindowSeconds < 0) {
                // No renewal window found, or invalid window given. Do the renewal
                LOG.debug("Invalid renewal window configured. Renewing token...");
                populateCredentials(credentials, topologyConf, topologyOwnerPrincipal);
                return;
            }

            long renewalWindow = TimeUnit.SECONDS.convert(renewalWindowSeconds, TimeUnit.MILLISECONDS);
            long nextRenewal = currentTime + renewalWindow;

            if (nextRenewal > expiration) {
                // Token will expire by the next renewal window. Do the renewal
                LOG.debug("ES Token will expire before next renewal window. Renewing token...");
                populateCredentials(credentials, topologyConf, topologyOwnerPrincipal);
                return;
            }
            LOG.debug("Token expiration is longer than renewal window. Token will be renewed at a later time.");
        } else {
            LOG.debug("Could not locate token to refresh!");
        }
    }

    /*
     * IAutoCredentials
     *
     * Interface that provides methods to pull credentials from the submission client (not a thing we do here) and
     * add any credentials to a Subject on a worker.
     */

    @Override
    public void populateCredentials(Map<String, String> credentials) {
        // This is run on the submission client. We won't end up doing anything here since everything important will run on Nimbus.
        if (LOG.isDebugEnabled()) {
            LOG.debug("Auto Credential Class Loaded on submission client");
        }
        credentials.put(ELASTICSEARCH_CREDENTIALS, "placeholder");
    }

    @Override
    public void populateSubject(Subject subject, Map<String, String> credentials) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Loading credentials to subject on worker side");
        }
        EsToken token = getCred(ELASTICSEARCH_CREDENTIALS, credentials);
        if (token != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("Loaded token [%s]. Adding to subject...", token.getName()));
            }
            User user = new JdkUser(subject, clusterSettings);
            user.addEsToken(token);
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Found no credentials to load");
            }
        }
    }

    @Override
    public void updateSubject(Subject subject, Map<String, String> credentials) {
        // Does exact same as populateSubject
        if (LOG.isDebugEnabled()) {
            LOG.debug("Performing subject update");
        }
        populateSubject(subject, credentials);
    }

    /*
     * Utility methods
     */

    private void putCred(String key, EsToken token, Map<String, String> credentials) {
        FastByteArrayOutputStream stream = new FastByteArrayOutputStream();
        DataOutput output = new DataOutputStream(stream);
        try {
            token.writeOut(output);
        } catch (IOException e) {
            throw new EsHadoopException("Could not serialize EsToken", e);
        }
        String credential = new String(Base64.encodeBase64(stream.bytes().bytes()), StringUtils.UTF_8);
        credentials.put(key, credential);
    }

    private EsToken getCred(String key, Map<String, String> credentials) {
        EsToken token = null;
        String serializedToken = credentials.get(key);
        if (serializedToken != null && !serializedToken.equals("placeholder")) {
            byte[] rawData = Base64.decodeBase64(serializedToken);
            try {
                token = new EsToken(new DataInputStream(new FastByteArrayInputStream(rawData)));
            } catch (IOException e) {
                throw new EsHadoopException("Could not deserialize EsToken", e);
            }
        }
        return token;
    }
}
