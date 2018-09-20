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

package org.elasticsearch.hadoop.gradle.fixture.hadoop

/**
 * Uniquely identifies an instance of a service in a Hadoop ecosystem
 *
 * Example: hive:hiveserver or hadoop:hdfs:namenode or hadoop:yarn:nodemanager
 */
class ServiceIdentifier {

    /**
     * The project/group that this service belongs to. Example: hadoop, hive
     */
    String serviceName

    /**
     * The subproject/subgroup that this service belongs to. Example: hdfs, mapreduce, yarn
     */
    String subGroup

    /**
     * The name of the service being run. Example: namenode, nodemanager, hiveserver
     */
    String roleName

    @Override
    String toString() {
        return "$serviceName:${subGroup == null ? '' : subGroup + ':'}${roleName}"
    }
}
