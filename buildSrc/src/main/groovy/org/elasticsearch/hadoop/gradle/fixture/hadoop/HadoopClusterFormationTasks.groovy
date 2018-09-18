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

import org.elasticsearch.gradle.Version
import org.elasticsearch.hadoop.gradle.VersionProperties;
import org.gradle.api.Project;
import org.gradle.api.Task
import org.gradle.api.artifacts.Configuration;

/**
 * A helper for creating tasks to build a cluster that is used by a task, and tear down the cluster
 * when the task is finished.
 */
class HadoopClusterFormationTasks {

//    /**
//     * Adds dependent tasks to the given task to start and stop a cluster with the given configuration.
//     * <p>
//     * Returns a list of NodeInfo objects for each node in the cluster.
//     *
//     * Based on {@link org.elasticsearch.gradle.test.ClusterFormationTasks}
//     */
//    static List<?> setup(Project project, String prefix, Task runner, HadoopClusterConfiguration config) {
//        // - do we need a shared dir?
//        Object startDependencies = config.dependencies
//        /*
//         * First clean the environment
//         */
//        // - not sure if we have a shared directory to clean anyway, so...
//
//        List<Task> startTasks = []
//        List<?> nodes = []
//
//        // - check node ranges, (num nodes >= bwc nodes)
//        // - require bwc version on non zero bwc node count
//
//        // This is our current version distribution configuration we use for all kinds of REST tests etc.
//        Configuration currentDistro = project.configurations.create("${prefix}_hadoopDistro")
//        // - add one for bwc
//        // - no need to care about oss-zip yet.
//        configureDistributionDependency(project, 'tar', currentDistro, "Hadoop Version Here")
//
//    }
//
//    static void configureDistributionDependency(Project project, String distro, Configuration configuration, String hadoopVersion) {
//        // - Don't need oss checking
//        String packaging = distro
//        // distro = tar vs zip = pick a packaging file suffix
//        String subgroup = distro
//        String artifactName = "hadoop" // Right?
//        project.dependencies.add(configuration.name, "") // Gotta add the remote zip/tar as a dependency some how.
//    }

}
