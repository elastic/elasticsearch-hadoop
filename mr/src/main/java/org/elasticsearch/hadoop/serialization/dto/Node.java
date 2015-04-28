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
package org.elasticsearch.hadoop.serialization.dto;

import java.io.Serializable;
import java.util.Map;

public class Node implements Serializable {

    private String id;
    private String name;
    private boolean hasHttp;
    private String ipAddress;
    private int httpPort;
    private Map<String, Object> attributes;
    private boolean isClient = false;

    public Node(String id, Map<String, Object> data) {
        this.id = id;
        name = data.get("name").toString();
        Object http = data.get("http_address");
        hasHttp = (http != null);

        attributes = (Map<String, Object>) data.get("attributes");
        if (attributes != null) {
            isClient = ("false".equals(attributes.get("data")) && "false".equals(attributes.get("master")));
        }

        if (!hasHttp) {
            return;
        }
        String httpAddr = http.toString();
        // strip ip address - regex would work but it's overkill
        int startIndex = httpAddr.indexOf("/") + 1;
        int endIndex = httpAddr.indexOf(":");
        ipAddress = httpAddr.substring(startIndex, endIndex);
        httpPort = Integer.valueOf(httpAddr.substring(endIndex + 1, httpAddr.indexOf("]")));
    }

    public boolean hasHttp() {
        return hasHttp;
    }

    public boolean isClient() {
        return isClient;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public int getHttpPort() {
        return httpPort;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public String getInet() {
        return ipAddress + ":" + httpPort;
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Node other = (Node) obj;
        if (id == null) {
            if (other.id != null)
                return false;
        }
        else if (!id.equals(other.id))
            return false;
        return true;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Node[id=").append(id).append(", name=").append(name).append(", ipAddress=").append(ipAddress)
        .append(", httpPort=").append(httpPort).append("]");
        return builder.toString();
    }
}