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

package org.elasticsearch.hadoop.gradle.util;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.net.URLConnection;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.gradle.api.GradleException;
import org.gradle.api.Project;

/**
 * A hacked together HTTPS based Ant Condition that allows for reading HTTPS data without
 * needing to trust the certificate of the server. Enabling security on hadoop means that
 * all HTTP traffic MUST be served over HTTPS, and the certificates that we use for testing
 * are not trusted by the default ant conditions because they are not in the truststore.
 */
public class WaitForURL {

    private static class TrustAllManager implements X509TrustManager {
        @Override
        public void checkClientTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
            // Trust everything!
        }

        @Override
        public void checkServerTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
            // Trust everything!
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return null; // Whom'st?
        }
    }

    private static class TrustAllVerifier implements HostnameVerifier {
        @Override
        public boolean verify(String hostName, SSLSession sslSession) {
            return true;
        }
    }

    private static SSLSocketFactory trustAllSSLSocketFactory;

    static SSLSocketFactory getTrustAllSSLSocketFactory() {
        if (trustAllSSLSocketFactory == null) {
            SSLContext trustAllSSLContext;
            try {
                trustAllSSLContext = SSLContext.getInstance("SSL");
            } catch (NoSuchAlgorithmException e) {
                throw new GradleException("Could not get SSL context", e);
            }
            try {
                trustAllSSLContext.init(null, new TrustManager[]{new TrustAllManager()}, new java.security.SecureRandom());
            } catch (KeyManagementException e) {
                throw new GradleException("Could not initialize SSL context", e);
            }
            trustAllSSLSocketFactory = trustAllSSLContext.getSocketFactory();
        }
        return trustAllSSLSocketFactory;
    }

    private String url = null;
    private boolean trustAllCerts = false;
    private boolean trustAllHostnames = false;
    private long totalWaitTimeMillis;
    private long checkEveryMillis;

    public WaitForURL setUrl(String url) {
        this.url = url;
        return this;
    }

    public WaitForURL setTrustAllSSLCerts(boolean trustAll) {
        this.trustAllCerts = trustAll;
        return this;
    }

    public WaitForURL setTrustAllHostnames(boolean trustAll) {
        this.trustAllHostnames = trustAll;
        return this;
    }

    public WaitForURL setTotalWaitTime(long maxWaitTime, TimeUnit unit) {
        this.totalWaitTimeMillis = unit.toMillis(maxWaitTime);
        return this;
    }

    public WaitForURL setCheckEvery(long checkEvery, TimeUnit unit) {
        this.checkEveryMillis = unit.toMillis(checkEvery);
        return this;
    }

    public boolean checkAvailability(Project project) throws GradleException {
        try {
            long start = System.currentTimeMillis();
            long end = start + totalWaitTimeMillis;

            while(System.currentTimeMillis() < end) {
                if (doCheckAvailability(project)) {
                    project.getLogger().debug("Got successful response from " + url);
                    return true;
                }

                Thread.sleep(checkEveryMillis);
            }
        } catch (InterruptedException var10) {
            project.getLogger().debug(", treating as timed out.");
        }

        project.getLogger().debug("Timed out waiting for successful response from " + url);
        return false;
    }

    private boolean doCheckAvailability(Project project) throws GradleException {
        if (url == null) {
            throw new GradleException("No url specified");
        }
        project.getLogger().debug("Https Condition checking " + url);
        try {
            URL url = new URL(this.url);
            try {
                URLConnection conn = url.openConnection();
                if (conn instanceof HttpURLConnection) {
                    HttpURLConnection http = (HttpURLConnection) conn;
                    http.setRequestMethod("GET");
                    http.setInstanceFollowRedirects(true);
                    if (http instanceof HttpsURLConnection) {
                        HttpsURLConnection https = (HttpsURLConnection) http;
                        if (trustAllCerts) {
                            https.setSSLSocketFactory(getTrustAllSSLSocketFactory());
                        }
                        if (trustAllHostnames) {
                            https.setHostnameVerifier(new TrustAllVerifier());
                        }
                    }
                    int code = http.getResponseCode();
                    project.getLogger().debug("Result code for " + this.url + " was " + code);
                    return code > 0 && code < 400;
                } else {
                    throw new GradleException("Provided URL was not HTTP or HTTPS: " + url);
                }
            } catch (ProtocolException e) {
                throw new GradleException("Could not perform GET", e);
            } catch (IOException e) {
                project.getLogger().debug("Could not retrieve URL: " + e.getMessage());
                return false;
            }
        } catch (MalformedURLException e) {
            throw new GradleException("Badly formed URL: " + url, e);
        }
    }
}
