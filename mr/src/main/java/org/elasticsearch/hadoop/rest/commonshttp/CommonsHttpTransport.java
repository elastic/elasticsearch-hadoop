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
package org.elasticsearch.hadoop.rest.commonshttp;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.EsHadoopIllegalStateException;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.rest.DelegatingInputStream;
import org.elasticsearch.hadoop.rest.EsHadoopInvalidRequest;
import org.elasticsearch.hadoop.rest.EsHadoopTransportException;
import org.elasticsearch.hadoop.rest.HeaderProcessor;
import org.elasticsearch.hadoop.rest.Request;
import org.elasticsearch.hadoop.rest.Response;
import org.elasticsearch.hadoop.rest.ReusableInputStream;
import org.elasticsearch.hadoop.rest.SimpleResponse;
import org.elasticsearch.hadoop.rest.Transport;
import org.elasticsearch.hadoop.rest.commonshttp.auth.EsHadoopAuthPolicies;
import org.elasticsearch.hadoop.rest.commonshttp.auth.bearer.EsApiKeyAuthScheme;
import org.elasticsearch.hadoop.rest.commonshttp.auth.bearer.EsApiKeyCredentials;
import org.elasticsearch.hadoop.rest.commonshttp.auth.spnego.SpnegoAuthScheme;
import org.elasticsearch.hadoop.rest.commonshttp.auth.spnego.SpnegoCredentials;
import org.elasticsearch.hadoop.rest.stats.Stats;
import org.elasticsearch.hadoop.rest.stats.StatsAware;
import org.elasticsearch.hadoop.security.SecureSettings;
import org.elasticsearch.hadoop.security.User;
import org.elasticsearch.hadoop.security.UserProvider;
import org.elasticsearch.hadoop.thirdparty.apache.commons.httpclient.Credentials;
import org.elasticsearch.hadoop.thirdparty.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.elasticsearch.hadoop.thirdparty.apache.commons.httpclient.Header;
import org.elasticsearch.hadoop.thirdparty.apache.commons.httpclient.HostConfiguration;
import org.elasticsearch.hadoop.thirdparty.apache.commons.httpclient.HttpClient;
import org.elasticsearch.hadoop.thirdparty.apache.commons.httpclient.HttpConnection;
import org.elasticsearch.hadoop.thirdparty.apache.commons.httpclient.HttpConnectionManager;
import org.elasticsearch.hadoop.thirdparty.apache.commons.httpclient.HttpMethod;
import org.elasticsearch.hadoop.thirdparty.apache.commons.httpclient.HttpState;
import org.elasticsearch.hadoop.thirdparty.apache.commons.httpclient.HttpStatus;
import org.elasticsearch.hadoop.thirdparty.apache.commons.httpclient.SimpleHttpConnectionManager;
import org.elasticsearch.hadoop.thirdparty.apache.commons.httpclient.URI;
import org.elasticsearch.hadoop.thirdparty.apache.commons.httpclient.URIException;
import org.elasticsearch.hadoop.thirdparty.apache.commons.httpclient.UsernamePasswordCredentials;
import org.elasticsearch.hadoop.thirdparty.apache.commons.httpclient.auth.AuthChallengeParser;
import org.elasticsearch.hadoop.thirdparty.apache.commons.httpclient.auth.AuthPolicy;
import org.elasticsearch.hadoop.thirdparty.apache.commons.httpclient.auth.AuthScheme;
import org.elasticsearch.hadoop.thirdparty.apache.commons.httpclient.auth.AuthScope;
import org.elasticsearch.hadoop.thirdparty.apache.commons.httpclient.auth.AuthState;
import org.elasticsearch.hadoop.thirdparty.apache.commons.httpclient.methods.EntityEnclosingMethod;
import org.elasticsearch.hadoop.thirdparty.apache.commons.httpclient.methods.GetMethod;
import org.elasticsearch.hadoop.thirdparty.apache.commons.httpclient.methods.HeadMethod;
import org.elasticsearch.hadoop.thirdparty.apache.commons.httpclient.methods.PostMethod;
import org.elasticsearch.hadoop.thirdparty.apache.commons.httpclient.methods.PutMethod;
import org.elasticsearch.hadoop.thirdparty.apache.commons.httpclient.params.HttpClientParams;
import org.elasticsearch.hadoop.thirdparty.apache.commons.httpclient.params.HttpConnectionManagerParams;
import org.elasticsearch.hadoop.thirdparty.apache.commons.httpclient.params.HttpMethodParams;
import org.elasticsearch.hadoop.thirdparty.apache.commons.httpclient.protocol.Protocol;
import org.elasticsearch.hadoop.thirdparty.apache.commons.httpclient.protocol.ProtocolSocketFactory;
import org.elasticsearch.hadoop.thirdparty.apache.commons.httpclient.protocol.SecureProtocolSocketFactory;
import org.elasticsearch.hadoop.util.ByteSequence;
import org.elasticsearch.hadoop.util.ReflectionUtils;
import org.elasticsearch.hadoop.util.StringUtils;
import org.elasticsearch.hadoop.util.encoding.HttpEncodingTools;

import javax.security.auth.kerberos.KerberosPrincipal;
import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.Socket;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Transport implemented on top of Commons Http. Provides transport retries.
 */
public class CommonsHttpTransport implements Transport, StatsAware {

    private static final String WWW_AUTHENTICATE = "WWW-Authenticate";

    private static Log log = LogFactory.getLog(CommonsHttpTransport.class);
    private static final Method GET_SOCKET;

    static {
        GET_SOCKET = ReflectionUtils.findMethod(HttpConnection.class, "getSocket", (Class[]) null);
        ReflectionUtils.makeAccessible(GET_SOCKET);
    }


    private final HttpClient client;
    private final HeaderProcessor headers;
    protected Stats stats = new Stats();
    private HttpConnection conn;
    private String proxyInfo = "";
    private final String httpInfo;
    private final boolean sslEnabled;
    private final String pathPrefix;
    private final Settings settings;
    private final SecureSettings secureSettings;
    private final String clusterName;
    private final UserProvider userProvider;
    private UserProvider proxyUserProvider = null;
    private String runAsUser = null;

    /** If the HTTP Connection is made through a proxy */
    private boolean isProxied = false;

    /** If the Socket Factory used for HTTP Connections extends SecureProtocolSocketFactory */
    private boolean isSecure = false;

    private static class ResponseInputStream extends DelegatingInputStream implements ReusableInputStream {

        private final HttpMethod method;
        private final boolean reusable;

        public ResponseInputStream(HttpMethod http) throws IOException {
            super(http.getResponseBodyAsStream());
            this.method = http;
            reusable = (delegate() instanceof ByteArrayInputStream);
        }

        @Override
        public int hashCode() {
            return super.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return super.equals(obj);
        }

        @Override
        public InputStream copy() {
            try {
                return (reusable ? method.getResponseBodyAsStream() : null);
            } catch (IOException ex) {
                throw new EsHadoopIllegalStateException(ex);
            }
        }

        @Override
        public void close() throws IOException {
            if (!isNull()) {
                try {
                    super.close();
                } catch (IOException e) {
                    // silently ignore
                }
            }
            method.releaseConnection();
        }
    }

    private class SocketTrackingConnectionManager extends SimpleHttpConnectionManager {

        @Override
        public HttpConnection getConnectionWithTimeout(HostConfiguration hostConfiguration, long timeout) {
            conn = super.getConnectionWithTimeout(hostConfiguration, timeout);
            return conn;
        }

        public void close() {
            if (httpConnection != null) {
                if (httpConnection.isOpen()) {
                    releaseConnection(httpConnection);
                }

                httpConnection.close();
            }

            httpConnection = null;
            conn = null;
        }
    }

    public CommonsHttpTransport(Settings settings, String host) {
        this(settings, new SecureSettings(settings), host);
    }

    public CommonsHttpTransport(Settings settings, SecureSettings secureSettings, String host) {
        if (log.isDebugEnabled()) {
            log.debug("Creating new CommonsHttpTransport");
        }
        this.settings = settings;
        this.secureSettings = secureSettings;
        this.clusterName = settings.getClusterInfoOrUnnamedLatest().getClusterName().getName(); // May be a bootstrap client.
        if (StringUtils.hasText(settings.getSecurityUserProviderClass())) {
            this.userProvider = UserProvider.create(settings);
        } else {
            this.userProvider = null;
        }
        httpInfo = host;
        sslEnabled = settings.getNetworkSSLEnabled();

        String pathPref = settings.getNodesPathPrefix();
        pathPrefix = (StringUtils.hasText(pathPref) ? addLeadingSlashIfNeeded(StringUtils.trimWhitespace(pathPref)) : StringUtils.trimWhitespace(pathPref));

        HttpClientParams params = new HttpClientParams();
        params.setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(
                settings.getHttpRetries(), false) {

            @Override
            public boolean retryMethod(HttpMethod method, IOException exception, int executionCount) {
                if (super.retryMethod(method, exception, executionCount)) {
                    stats.netRetries++;
                    return true;
                }
                return false;
            }
        });

        // Max time to wait for a connection from the connectionMgr pool
        params.setConnectionManagerTimeout(settings.getHttpTimeout());
        // Max time to wait for data from a connection.
        params.setSoTimeout((int) settings.getHttpTimeout());
        // explicitly set the charset
        params.setCredentialCharset(StringUtils.UTF_8.name());
        params.setContentCharset(StringUtils.UTF_8.name());

        HostConfiguration hostConfig = new HostConfiguration();

        hostConfig = setupSSLIfNeeded(settings, secureSettings, hostConfig);
        hostConfig = setupSocksProxy(settings, secureSettings, hostConfig);
        Object[] authSettings = setupHttpOrHttpsProxy(settings, secureSettings, hostConfig);
        hostConfig = (HostConfiguration) authSettings[0];

        try {
            hostConfig.setHost(new URI(escapeUri(host, sslEnabled), false));
        } catch (IOException ex) {
            throw new EsHadoopTransportException("Invalid target URI " + host, ex);
        }
        client = new HttpClient(params, new SocketTrackingConnectionManager());
        client.setHostConfiguration(hostConfig);

        addHttpAuth(settings, secureSettings, authSettings);
        completeAuth(authSettings);

        HttpConnectionManagerParams connectionParams = client.getHttpConnectionManager().getParams();
        // make sure to disable Nagle's protocol
        connectionParams.setTcpNoDelay(true);
        // Max time to wait to establish an initial HTTP connection
        connectionParams.setConnectionTimeout((int) settings.getHttpTimeout());

        this.headers = new HeaderProcessor(settings);

        if (log.isTraceEnabled()) {
            log.trace("Opening HTTP transport to " + httpInfo);
        }
    }

    private HostConfiguration setupSSLIfNeeded(Settings settings, SecureSettings secureSettings, HostConfiguration hostConfig) {
        if (!sslEnabled) {
            return hostConfig;
        }

        // we actually have a socks proxy, let's start the setup
        if (log.isDebugEnabled()) {
            log.debug("SSL Connection enabled");
        }

        isSecure = true;

        //
        // switch protocol
        // due to how HttpCommons work internally this dance is best to be kept as is
        //
        String schema = "https";
        int port = 443;
        SecureProtocolSocketFactory sslFactory = new SSLSocketFactory(settings, secureSettings);

        replaceProtocol(sslFactory, schema, port);

        return hostConfig;
    }

    private void addHttpAuth(Settings settings, SecureSettings secureSettings, Object[] authSettings) {
        List<String> authPrefs = new ArrayList<String>();
        if (StringUtils.hasText(settings.getNetworkHttpAuthUser())) {
            HttpState state = (authSettings[1] != null ? (HttpState) authSettings[1] : new HttpState());
            authSettings[1] = state;
            // TODO: Limit this by hosts and ports
            AuthScope scope = new AuthScope(AuthScope.ANY_HOST, AuthScope.ANY_PORT, AuthScope.ANY_REALM, AuthPolicy.BASIC);
            Credentials usernamePassword = new UsernamePasswordCredentials(settings.getNetworkHttpAuthUser(),
                    secureSettings.getSecureProperty(ConfigurationOptions.ES_NET_HTTP_AUTH_PASS));
            state.setCredentials(scope, usernamePassword);
            if (log.isDebugEnabled()) {
                log.debug("Using detected HTTP Auth credentials...");
            }
            authPrefs.add(AuthPolicy.BASIC);
            client.getParams().setAuthenticationPreemptive(true); // Preemptive auth only if there's basic creds.
        }
        // Try auth schemes based on currently logged in user:
        if (userProvider != null) {
            User user = userProvider.getUser();
            // Add ApiKey Authentication if a key is present
            if (log.isDebugEnabled()) {
                log.debug("checking for token using cluster name [" + clusterName + "]");
            }
            if (user.getEsToken(clusterName) != null) {
                HttpState state = (authSettings[1] != null ? (HttpState) authSettings[1] : new HttpState());
                authSettings[1] = state;
                // TODO: Limit this by hosts and ports
                AuthScope scope = new AuthScope(AuthScope.ANY_HOST, AuthScope.ANY_PORT, AuthScope.ANY_REALM, EsHadoopAuthPolicies.APIKEY);
                Credentials tokenCredentials = new EsApiKeyCredentials(userProvider, clusterName);
                state.setCredentials(scope, tokenCredentials);
                if (log.isDebugEnabled()) {
                    log.debug("Using detected Token credentials...");
                }
                EsHadoopAuthPolicies.registerAuthSchemes();
                authPrefs.add(EsHadoopAuthPolicies.APIKEY);
            } else if (userProvider.isEsKerberosEnabled()) {
                // Add SPNEGO auth if a kerberos principal exists on the user and the elastic principal is set
                // Only do this if a token does not exist on the current user.
                // The auth mode may say that it is Kerberos, but the client
                // could be running in a remote JVM that does not have the
                // Kerberos credentials available.
                if (!StringUtils.hasText(settings.getNetworkSpnegoAuthElasticsearchPrincipal())) {
                    throw new EsHadoopIllegalArgumentException("Missing Elasticsearch Kerberos Principal name. " +
                            "Specify one with [" + ConfigurationOptions.ES_NET_SPNEGO_AUTH_ELASTICSEARCH_PRINCIPAL + "]");
                }

                // Pick the appropriate user provider to get credentials from for SPNEGO auth
                UserProvider credentialUserProvider;
                if (user.isProxyUser()) {
                    // If the user is a proxy user, get a provider for the real
                    // user and capture the proxy user's name to impersonate
                    proxyUserProvider = user.getRealUserProvider();
                    runAsUser = user.getUserName();

                    // Ensure that this real user even has Kerberos Creds:
                    User realUser = proxyUserProvider.getUser();
                    KerberosPrincipal realPrincipal = realUser.getKerberosPrincipal();
                    if (realPrincipal == null) {
                        throw new EsHadoopIllegalArgumentException("Could not locate Kerberos Principal on real user [" +
                                realUser.getUserName() + "] underneath proxy user [" + runAsUser + "]");
                    }

                    if (log.isDebugEnabled()) {
                        log.debug("Using detected SPNEGO credentials for real user [" + realUser.getUserName() + "] to proxy as [" +
                                runAsUser + "]...");
                    }
                    credentialUserProvider = proxyUserProvider;
                } else if (user.getKerberosPrincipal() != null) {
                    // Ensure that the user principal exists
                    if (log.isDebugEnabled()) {
                        log.debug("Using detected SPNEGO credentials for user [" + user.getUserName() + "]...");
                    }
                    credentialUserProvider = userProvider;
                } else {
                    throw new EsHadoopIllegalArgumentException("Could not locate Kerberos Principal on currently logged in user.");
                }

                // Add the user provider to credentials
                HttpState state = (authSettings[1] != null ? (HttpState) authSettings[1] : new HttpState());
                authSettings[1] = state;
                // TODO: Limit this by hosts and ports
                AuthScope scope = new AuthScope(AuthScope.ANY_HOST, AuthScope.ANY_PORT, AuthScope.ANY_REALM, EsHadoopAuthPolicies.NEGOTIATE);
                // TODO: This should just pass in the user provider instead of getting the user principal at this point.
                Credentials credential = new SpnegoCredentials(credentialUserProvider, settings.getNetworkSpnegoAuthElasticsearchPrincipal());
                state.setCredentials(scope, credential);
                EsHadoopAuthPolicies.registerAuthSchemes();
                authPrefs.add(EsHadoopAuthPolicies.NEGOTIATE);
            }
        } else {
            if (log.isDebugEnabled()) {
                log.debug("No UserProvider configured. Skipping Kerberos/Token auth settings");
            }
        }
        if (log.isDebugEnabled()) {
            log.debug("Using auth prefs: [" + authPrefs + "]");
        }
        client.getParams().setParameter(AuthPolicy.AUTH_SCHEME_PRIORITY, authPrefs);
    }

    private void completeAuth(Object[] authSettings) {
        if (authSettings[1] != null) {
            client.setState((HttpState) authSettings[1]);
        }
    }

    private Object[] setupHttpOrHttpsProxy(Settings settings, SecureSettings secureSettings, HostConfiguration hostConfig) {
        // return HostConfiguration + HttpState
        Object[] results = new Object[2];
        results[0] = hostConfig;
        // set proxy settings
        String proxyHost = null;
        int proxyPort = -1;

        if (sslEnabled) {
            if (settings.getNetworkHttpsUseSystemProperties()) {
                proxyHost = System.getProperty("https.proxyHost");
                proxyPort = Integer.getInteger("https.proxyPort", -1);
            }
            if (StringUtils.hasText(settings.getNetworkProxyHttpsHost())) {
                proxyHost = settings.getNetworkProxyHttpsHost();
            }
            if (settings.getNetworkProxyHttpsPort() > 0) {
                proxyPort = settings.getNetworkProxyHttpsPort();
            }
        }
        else {
            if (settings.getNetworkHttpUseSystemProperties()) {
                proxyHost = System.getProperty("http.proxyHost");
                proxyPort = Integer.getInteger("http.proxyPort", -1);
            }
            if (StringUtils.hasText(settings.getNetworkProxyHttpHost())) {
                proxyHost = settings.getNetworkProxyHttpHost();
            }
            if (settings.getNetworkProxyHttpPort() > 0) {
                proxyPort = settings.getNetworkProxyHttpPort();
            }
        }

        if (StringUtils.hasText(proxyHost)) {
            hostConfig.setProxy(proxyHost, proxyPort);
            isProxied = true;
            proxyInfo = proxyInfo.concat(String.format(Locale.ROOT, "[%s proxy %s:%s]", (sslEnabled ? "HTTPS" : "HTTP"), proxyHost, proxyPort));

            // client is not yet initialized so postpone state
            if (sslEnabled) {
                if (StringUtils.hasText(settings.getNetworkProxyHttpsUser())) {
                    if (!StringUtils.hasText(secureSettings.getSecureProperty(ConfigurationOptions.ES_NET_PROXY_HTTPS_PASS))) {
                        log.warn(String.format("HTTPS proxy user specified but no/empty password defined - double check the [%s] property", ConfigurationOptions.ES_NET_PROXY_HTTPS_PASS));
                    }
                    HttpState state = new HttpState();
                    state.setProxyCredentials(AuthScope.ANY, new UsernamePasswordCredentials(settings.getNetworkProxyHttpsUser(), secureSettings.getSecureProperty(ConfigurationOptions.ES_NET_PROXY_HTTPS_PASS)));
                    // client is not yet initialized so simply save the object for later
                    results[1] = state;
                }
    
                if (log.isDebugEnabled()) {
                    if (StringUtils.hasText(settings.getNetworkProxyHttpsUser())) {
                        log.debug(String.format("Using authenticated HTTPS proxy [%s:%s]", proxyHost, proxyPort));
                    }
                    else {
                        log.debug(String.format("Using HTTPS proxy [%s:%s]", proxyHost, proxyPort));
                    }
                }
            }
            else {
                if (StringUtils.hasText(settings.getNetworkProxyHttpUser())) {
                    if (!StringUtils.hasText(secureSettings.getSecureProperty(ConfigurationOptions.ES_NET_PROXY_HTTP_PASS))) {
                        log.warn(String.format("HTTP proxy user specified but no/empty password defined - double check the [%s] property", ConfigurationOptions.ES_NET_PROXY_HTTP_PASS));
                    }
                    HttpState state = new HttpState();
                    state.setProxyCredentials(AuthScope.ANY, new UsernamePasswordCredentials(settings.getNetworkProxyHttpUser(), secureSettings.getSecureProperty(ConfigurationOptions.ES_NET_PROXY_HTTP_PASS)));
                    // client is not yet initialized so simply save the object for later
                    results[1] = state;
                }
    
                if (log.isDebugEnabled()) {
                    if (StringUtils.hasText(settings.getNetworkProxyHttpUser())) {
                        log.debug(String.format("Using authenticated HTTP proxy [%s:%s]", proxyHost, proxyPort));
                    }
                    else {
                        log.debug(String.format("Using HTTP proxy [%s:%s]", proxyHost, proxyPort));
                    }
                }
            }
        }

        return results;
    }

    private HostConfiguration setupSocksProxy(Settings settings, SecureSettings secureSettings, HostConfiguration hostConfig) {
        // set proxy settings
        String proxyHost = null;
        int proxyPort = -1;
        String proxyUser = null;
        String proxyPass = null;

        if (settings.getNetworkHttpUseSystemProperties()) {
            proxyHost = System.getProperty("socksProxyHost");
            proxyPort = Integer.getInteger("socksProxyPort", -1);
            proxyUser = System.getProperty("java.net.socks.username");
            proxyPass = System.getProperty("java.net.socks.password");
        }
        if (StringUtils.hasText(settings.getNetworkProxySocksHost())) {
            proxyHost = settings.getNetworkProxySocksHost();
        }
        if (settings.getNetworkProxySocksPort() > 0) {
            proxyPort = settings.getNetworkProxySocksPort();
        }
        if (StringUtils.hasText(settings.getNetworkProxySocksUser())) {
            proxyUser = settings.getNetworkProxySocksUser();
        }
        if (StringUtils.hasText(secureSettings.getSecureProperty(ConfigurationOptions.ES_NET_PROXY_SOCKS_PASS))) {
            proxyPass = secureSettings.getSecureProperty(ConfigurationOptions.ES_NET_PROXY_SOCKS_PASS);
        }

        // we actually have a socks proxy, let's start the setup
        if (StringUtils.hasText(proxyHost)) {
            log.warn("Connecting to Elasticsearch through SOCKS proxy is deprecated in 6.6.0 and will be removed in a later release.");
            isSecure = false;
            isProxied = true;
            proxyInfo = proxyInfo.concat(String.format("[SOCKS proxy %s:%s]", proxyHost, proxyPort));

            if (!StringUtils.hasText(proxyUser)) {
                log.warn(String.format(
                        "SOCKS proxy user specified but no/empty password defined - double check the [%s] property",
                        ConfigurationOptions.ES_NET_PROXY_SOCKS_PASS));
            }

            if (log.isDebugEnabled()) {
                if (StringUtils.hasText(proxyUser)) {
                    log.debug(String.format("Using authenticated SOCKS proxy [%s:%s]", proxyHost, proxyPort));
                }
                else {
                    log.debug(String.format("Using SOCKS proxy [%s:%s]", proxyHost, proxyPort));
                }
            }

            //
            // switch protocol
            // due to how HttpCommons work internally this dance is best to be kept as is
            //
            String schema = sslEnabled ? "https" : "http";
            int port = sslEnabled ? 443 : 80;
            SocksSocketFactory socketFactory = new SocksSocketFactory(proxyHost, proxyPort, proxyUser, proxyPass);
            replaceProtocol(socketFactory, schema, port);
        }

        return hostConfig;
    }

    static void replaceProtocol(ProtocolSocketFactory socketFactory, String schema, int defaultPort) {
        //
        // switch protocol
        // due to how HttpCommons work internally this dance is best to be kept as is
        //

        Protocol directHttp = Protocol.getProtocol(schema);
        if (directHttp instanceof DelegatedProtocol) {
            // unwrap the original
            directHttp = ((DelegatedProtocol)directHttp).getOriginal();
            assert directHttp instanceof DelegatedProtocol == false;
        }
        Protocol proxiedHttp = new DelegatedProtocol(socketFactory, directHttp, schema, defaultPort);
        // NB: register the new protocol since when using absolute URIs, HttpClient#executeMethod will override the configuration (#387)
        // NB: hence why the original/direct http protocol is saved - as otherwise the connection is not closed since it is considered different
        // NB: (as the protocol identities don't match)

        // this is not really needed since it's being replaced later on
        // hostConfig.setHost(proxyHost, proxyPort, proxiedHttp);
        Protocol.registerProtocol(schema, proxiedHttp);

        // end dance
    }

    @Override
    public Response execute(Request request) throws IOException {
        HttpMethod http = null;

        switch (request.method()) {
        case DELETE:
            http = new DeleteMethodWithBody();
            break;
        case HEAD:
            http = new HeadMethod();
            break;
        case GET:
            http = (request.body() == null ? new GetMethod() : new GetMethodWithBody());
            break;
        case POST:
            http = new PostMethod();
            break;
        case PUT:
            http = new PutMethod();
            break;

        default:
            throw new EsHadoopTransportException("Unknown request method " + request.method());
        }

        CharSequence uri = request.uri();
        if (StringUtils.hasText(uri)) {
            if (String.valueOf(uri).contains("?")) {
                throw new EsHadoopInvalidRequest("URI has query portion on it: [" + uri + "]");
            }
            http.setURI(new URI(escapeUri(uri.toString(), sslEnabled), false));
        }

        // NB: initialize the path _after_ the URI otherwise the path gets reset to /
        // add node prefix (if specified)
        String path = pathPrefix + addLeadingSlashIfNeeded(request.path().toString());
        if (path.contains("?")) {
            throw new EsHadoopInvalidRequest("Path has query portion on it: [" + path + "]");
        }

        path = HttpEncodingTools.encodePath(path);

        http.setPath(path);

        try {
            // validate new URI
            uri = http.getURI().toString();
        } catch (URIException uriex) {
            throw new EsHadoopTransportException("Invalid target URI " + request, uriex);
        }

        CharSequence params = request.params();
        if (StringUtils.hasText(params)) {
            http.setQueryString(params.toString());
        }

        ByteSequence ba = request.body();
        if (ba != null && ba.length() > 0) {
            if (!(http instanceof EntityEnclosingMethod)) {
                throw new IllegalStateException(String.format("Method %s cannot contain body - implementation bug", request.method().name()));
            }
            EntityEnclosingMethod entityMethod = (EntityEnclosingMethod) http;
            entityMethod.setRequestEntity(new BytesArrayRequestEntity(ba));
            entityMethod.setContentChunked(false);
        }

        headers.applyTo(http);

        // We don't want a token added from a proxy user to collide with the
        // run_as mechanism from a real user impersonating said proxy, so
        // make these conditions mutually exclusive.
        if (runAsUser != null) {
            if (log.isDebugEnabled()) {
                log.debug("Performing request with runAs user set to ["+runAsUser+"]");
            }
            http.addRequestHeader("es-security-runas-user", runAsUser);
        } else if (userProvider != null && userProvider.getUser().getEsToken(clusterName) != null) {
            // If we are using token authentication, set the auth to be preemptive:
            if (log.isDebugEnabled()) {
                log.debug("Performing preemptive authentication with API Token");
            }
            http.getHostAuthState().setPreemptive();
            http.getHostAuthState().setAuthAttempted(true);
            http.getHostAuthState().setAuthScheme(new EsApiKeyAuthScheme());
            if (isProxied && !isSecure) {
                http.getProxyAuthState().setPreemptive();
                http.getProxyAuthState().setAuthAttempted(true);
            }
        }

        // Determine a user provider to use for executing, or if we even need one at all
        UserProvider executingProvider;
        if (proxyUserProvider != null) {
            log.debug("Using proxyUserProvider to wrap rest request");
            executingProvider = proxyUserProvider;
        } else if (userProvider != null) {
            log.debug("Using regular user provider to wrap rest request");
            executingProvider = userProvider;
        } else {
            log.debug("Skipping user provider request wrapping");
            executingProvider = null;
        }

        // when tracing, log everything
        if (log.isTraceEnabled()) {
            log.trace(String.format("Tx %s[%s]@[%s][%s]?[%s] w/ payload [%s]", proxyInfo, request.method().name(), httpInfo, request.path(), request.params(), request.body()));
        }

        if (executingProvider != null) {
            final HttpMethod method = http;
            executingProvider.getUser().doAs(new PrivilegedExceptionAction<Object>() {
                @Override
                public Object run() throws Exception {
                    doExecute(method);
                    return null;
                }
            });
        } else {
            doExecute(http);
        }

        if (log.isTraceEnabled()) {
            Socket sk = ReflectionUtils.invoke(GET_SOCKET, conn, (Object[]) null);
            String addr = sk.getLocalAddress().getHostAddress();
            log.trace(String.format("Rx %s@[%s] [%s-%s] [%s]", proxyInfo, addr, http.getStatusCode(), HttpStatus.getStatusText(http.getStatusCode()), http.getResponseBodyAsString()));
        }

        // Parse headers
        Map<String, List<String>> headers = new HashMap<>();
        for (Header responseHeader : http.getResponseHeaders()) {
            List<String> headerValues = headers.computeIfAbsent(responseHeader.getName(), k -> new ArrayList<>());
            headerValues.add(responseHeader.getValue());
        }

        // the request URI is not set (since it is retried across hosts), so use the http info instead for source
        return new SimpleResponse(http.getStatusCode(), new ResponseInputStream(http), httpInfo, headers);
    }

    /**
     * Actually perform the request
     * @param method the HTTP method to perform
     * @throws IOException If there is an issue during the method execution
     */
    private void doExecute(HttpMethod method) throws IOException {
        long start = System.currentTimeMillis();
        try {
            client.executeMethod(method);
            afterExecute(method);
        } finally {
            stats.netTotalTime += (System.currentTimeMillis() - start);
            closeAuthSchemeQuietly(method);
        }
    }

    /**
     * Close any authentication resources that we may still have open and perform any after-response duties that we need to perform.
     * @param method The method that has been executed
     * @throws IOException If any issues arise during post processing
     */
    private void afterExecute(HttpMethod method) throws IOException {
        AuthState hostAuthState = method.getHostAuthState();
        if (hostAuthState.isPreemptive() || hostAuthState.isAuthAttempted()) {
            AuthScheme authScheme = hostAuthState.getAuthScheme();

            if (authScheme instanceof SpnegoAuthScheme && settings.getNetworkSpnegoAuthMutual()) {
                // Perform Mutual Authentication
                SpnegoAuthScheme spnegoAuthScheme = ((SpnegoAuthScheme) authScheme);
                Map challenges = AuthChallengeParser.parseChallenges(method.getResponseHeaders(WWW_AUTHENTICATE));
                String id = spnegoAuthScheme.getSchemeName();
                String challenge = (String) challenges.get(id.toLowerCase());
                if (challenge == null) {
                    throw new IOException(id + " authorization challenge expected, but not found");
                }
                spnegoAuthScheme.ensureMutualAuth(challenge);
            }
        }
    }

    /**
     * Close the underlying authscheme if it is a Closeable object.
     * @param method Executing method
     * @throws IOException If the scheme could not be closed
     */
    private void closeAuthSchemeQuietly(HttpMethod method) {
        AuthScheme scheme = method.getHostAuthState().getAuthScheme();
        if (scheme instanceof Closeable) {
            try {
                ((Closeable) scheme).close();
            } catch (IOException e) {
                log.error("Could not close [" + scheme.getSchemeName() + "] auth scheme", e);
            }
        }
    }

    @Override
    public void close() {
        if (log.isTraceEnabled()) {
            log.trace("Closing HTTP transport to " + httpInfo);
        }

        HttpConnectionManager manager = client.getHttpConnectionManager();
        if (manager instanceof SocketTrackingConnectionManager) {
            try {
                ((SocketTrackingConnectionManager) manager).close();
            } catch (NullPointerException npe) {
                // ignore
            } catch (Exception ex) {
                // log - not much else to do
                log.warn("Exception closing underlying HTTP manager", ex);
            }
        }
    }

    private static String escapeUri(String uri, boolean ssl) {
        // escape the uri right away
        String escaped = HttpEncodingTools.encodeUri(uri);
        return escaped.contains("://") ? escaped : (ssl ? "https://" : "http://") + escaped;
    }

    private static String addLeadingSlashIfNeeded(String string) {
        return string.startsWith("/") ? string : "/" + string;
    }

    @Override
    public Stats stats() {
        return stats;
    }
}