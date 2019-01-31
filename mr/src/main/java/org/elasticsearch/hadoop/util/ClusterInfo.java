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
 * Response and container for information provided by a node's "main" root-level action.
 */
public class ClusterInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    private final ClusterName clusterName;
    private final EsMajorVersion majorVersion;

    public static ClusterInfo unnamedLatest() {
        return new ClusterInfo(new ClusterName(ClusterName.UNNAMED_CLUSTER_NAME, null), EsMajorVersion.LATEST);
    }

    public static ClusterInfo unnamedClusterWithVersion(EsMajorVersion version) {
        return new ClusterInfo(new ClusterName(ClusterName.UNNAMED_CLUSTER_NAME, null), version);
    }

    public ClusterInfo(ClusterName clusterName, EsMajorVersion majorVersion) {
        this.clusterName = clusterName;
        this.majorVersion = majorVersion;
    }

    public ClusterName getClusterName() {
        return clusterName;
    }

    public EsMajorVersion getMajorVersion() {
        return majorVersion;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ClusterInfo that = (ClusterInfo) o;

        if (clusterName != null ? !clusterName.equals(that.clusterName) : that.clusterName != null) return false;
        return majorVersion != null ? majorVersion.equals(that.majorVersion) : that.majorVersion == null;
    }

    @Override
    public int hashCode() {
        int result = clusterName != null ? clusterName.hashCode() : 0;
        result = 31 * result + (majorVersion != null ? majorVersion.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ClusterInfo{" +
                "clusterName=" + clusterName +
                ", majorVersion=" + majorVersion +
                '}';
    }
}
