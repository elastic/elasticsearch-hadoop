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

package org.elasticsearch.hadoop.security;

/**
 * Stores token authentication information for an Elasticsearch user.
 */
public class EsToken {

    private final String accessToken;
    private final long expiresIn;
    private final String refreshToken;
    private final String userName;

    public EsToken(String userName, String accessToken, String refreshToken, long expiresIn) {
        this.userName = userName;
        this.accessToken = accessToken;
        this.refreshToken = refreshToken;
        this.expiresIn = expiresIn;
    }

    public String getAccessToken() {
        return accessToken;
    }

    public long getExpiresIn() {
        return expiresIn;
    }

    public String getRefreshToken() {
        return refreshToken;
    }

    public String getUserName() {
        return userName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EsToken esToken = (EsToken) o;

        if (expiresIn != esToken.expiresIn) return false;
        if (accessToken != null ? !accessToken.equals(esToken.accessToken) : esToken.accessToken != null) return false;
        if (refreshToken != null ? !refreshToken.equals(esToken.refreshToken) : esToken.refreshToken != null) return false;
        return userName != null ? userName.equals(esToken.userName) : esToken.userName == null;
    }

    @Override
    public int hashCode() {
        int result = accessToken != null ? accessToken.hashCode() : 0;
        result = 31 * result + (int) (expiresIn ^ (expiresIn >>> 32));
        result = 31 * result + (refreshToken != null ? refreshToken.hashCode() : 0);
        result = 31 * result + (userName != null ? userName.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "EsToken{" +
                "accessToken='" + accessToken + '\'' +
                ", expireTime=" + expiresIn +
                ", refreshToken='" + refreshToken + '\'' +
                ", userName='" + userName + '\'' +
                '}';
    }
}
