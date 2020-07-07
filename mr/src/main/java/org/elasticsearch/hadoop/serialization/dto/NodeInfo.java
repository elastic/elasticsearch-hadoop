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

import org.elasticsearch.hadoop.util.EsMajorVersion;
import org.elasticsearch.hadoop.util.StringUtils;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Node information
 */
public class NodeInfo implements Serializable {
    private final String id;
    private final String name;
    private final String host;
    private final String ip;
    private final String publishAddress;
    private final boolean hasHttp;
    private final boolean isClient;
    private final boolean isData;
    private final boolean isIngest;

    public NodeInfo(String id, Map<String, Object> map) {
        this.id = id;
        EsMajorVersion version = EsMajorVersion.parse((String) map.get("version"));
        this.name = (String) map.get("name");
        this.host = (String) map.get("host");
        this.ip = (String) map.get("ip");
        if (version.before(EsMajorVersion.V_5_X)) {
            Map<String, Object> attributes = (Map<String, Object>) map.get("attributes");
            if (attributes == null) {
                this.isClient = false;
                this.isData = true;
            } else {
                String data = (String) attributes.get("data");
                this.isClient = data == null ? true : !Boolean.parseBoolean(data);
                this.isData = data == null ? true : Boolean.parseBoolean(data);
            }
            this.isIngest = false;
        } else {
            List<String> roles = (List<String>) map.get("roles");
            this.isClient = !roles.contains("data");
            this.isData = roles.contains("data");
            this.isIngest = roles.contains("ingest");
        }
        Map<String, Object> httpMap = (Map<String, Object>) map.get("http");
        if (httpMap != null) {
            String addr = (String) httpMap.get("publish_address");
            if (addr != null) {
                StringUtils.IpAndPort ipAndPort = StringUtils.parseIpAddress(addr);
                this.publishAddress = ipAndPort.ip + ":" + ipAndPort.port;
                this.hasHttp = true;
            } else {
                this.publishAddress = null;
                this.hasHttp = false;
            }
        } else {
            this.publishAddress = null;
            this.hasHttp = false;
        }
    }

    public boolean hasHttp() {
        return hasHttp;
    }

    public boolean isClient() {
        return isClient;
    }

    public boolean isData() {
        return isData;
    }

    public boolean isIngest() {
        return isIngest;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getHost() { return host; }

    public String getPublishAddress() {
        return publishAddress;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        NodeInfo nodeInfo = (NodeInfo) o;

        if (hasHttp != nodeInfo.hasHttp) return false;
        if (isClient != nodeInfo.isClient) return false;
        if (isData != nodeInfo.isData) return false;
        if (!id.equals(nodeInfo.id)) return false;
        if (!name.equals(nodeInfo.name)) return false;
        if (!host.equals(nodeInfo.host)) return false;
        if (!ip.equals(nodeInfo.ip)) return false;
        return publishAddress != null ? publishAddress.equals(nodeInfo.publishAddress) : nodeInfo.publishAddress == null;
    }

    @Override
    public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + name.hashCode();
        result = 31 * result + host.hashCode();
        result = 31 * result + ip.hashCode();
        result = 31 * result + (publishAddress != null ? publishAddress.hashCode() : 0);
        result = 31 * result + (hasHttp ? 1 : 0);
        result = 31 * result + (isClient ? 1 : 0);
        result = 31 * result + (isData ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        return "NodeInfo{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", host='" + host + '\'' +
                ", ip='" + ip + '\'' +
                ", publishAddress='" + publishAddress + '\'' +
                ", hasHttp=" + hasHttp +
                ", isClient=" + isClient +
                ", isData=" + isData +
                '}';
    }
}