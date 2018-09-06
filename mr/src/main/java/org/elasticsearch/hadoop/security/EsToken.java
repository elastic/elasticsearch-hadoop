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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Stores token authentication information for an Elasticsearch user.
 */
public class EsToken {

    private final String userName;
    private final String accessToken;
    private final String refreshToken;
    private final long expirationTime;
    private final String clusterName;

    public EsToken(String userName, String accessToken, String refreshToken, long expirationTime, String clusterName) {
        this.userName = userName;
        this.accessToken = accessToken;
        this.refreshToken = refreshToken;
        this.expirationTime = expirationTime;
        this.clusterName = clusterName;
    }

    public EsToken(DataInput inputStream) throws IOException {
        this.userName = inputStream.readUTF();
        this.accessToken = inputStream.readUTF();
        this.refreshToken = inputStream.readUTF();
        this.expirationTime = inputStream.readLong();
        this.clusterName = inputStream.readUTF();
    }

    public String getAccessToken() {
        return accessToken;
    }

    public long getExpirationTime() {
        return expirationTime;
    }

    public String getRefreshToken() {
        return refreshToken;
    }

    public String getUserName() {
        return userName;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void writeOut(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(userName);
        dataOutput.writeUTF(accessToken);
        dataOutput.writeUTF(refreshToken);
        dataOutput.writeLong(expirationTime);
        dataOutput.writeUTF(clusterName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EsToken esToken = (EsToken) o;

        if (expirationTime != esToken.expirationTime) return false;
        if (userName != null ? !userName.equals(esToken.userName) : esToken.userName != null) return false;
        if (accessToken != null ? !accessToken.equals(esToken.accessToken) : esToken.accessToken != null) return false;
        if (refreshToken != null ? !refreshToken.equals(esToken.refreshToken) : esToken.refreshToken != null) return false;
        return clusterName != null ? clusterName.equals(esToken.clusterName) : esToken.clusterName == null;
    }

    @Override
    public int hashCode() {
        int result = userName != null ? userName.hashCode() : 0;
        result = 31 * result + (accessToken != null ? accessToken.hashCode() : 0);
        result = 31 * result + (refreshToken != null ? refreshToken.hashCode() : 0);
        result = 31 * result + (int) (expirationTime ^ (expirationTime >>> 32));
        result = 31 * result + (clusterName != null ? clusterName.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "EsToken{" +
                "userName='" + userName + '\'' +
                ", accessToken='" + accessToken + '\'' +
                ", refreshToken='" + refreshToken + '\'' +
                ", expiresIn=" + expirationTime +
                ", clusterName='" + clusterName + '\'' +
                '}';
    }
}
