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

package org.elasticsearch.hadoop.util;

import java.io.Serializable;

/**
 * Container for a cluster's given name and UUID.
 */
public class ClusterName implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final String UNNAMED_CLUSTER_NAME = "!UNNAMED!";

    private final String clusterName;
    private final String clusterUUID;

    public ClusterName(String clusterName, String clusterUUID) {
        this.clusterName = clusterName;
        this.clusterUUID = clusterUUID;
    }

    public String getName() {
        return clusterName;
    }

    public String getUUID() {
        return clusterUUID;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ClusterName that = (ClusterName) o;

        if (clusterName != null ? !clusterName.equals(that.clusterName) : that.clusterName != null) return false;
        return clusterUUID != null ? clusterUUID.equals(that.clusterUUID) : that.clusterUUID == null;
    }

    @Override
    public int hashCode() {
        int result = clusterName != null ? clusterName.hashCode() : 0;
        result = 31 * result + (clusterUUID != null ? clusterUUID.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ClusterName{" +
                "clusterName='" + clusterName + '\'' +
                ", clusterUUID='" + clusterUUID + '\'' +
                '}';
    }
}
