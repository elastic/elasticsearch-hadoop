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

package org.elasticsearch.hadoop.gradle

import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.artifacts.Dependency
import org.gradle.api.artifacts.ExternalDependency
import org.gradle.api.file.CopySpec
import org.gradle.api.file.DuplicatesStrategy
import org.gradle.api.tasks.bundling.Jar
import org.gradle.api.tasks.bundling.Zip
import org.gradle.api.tasks.javadoc.Javadoc

/**
 * Plugin for configuring any project module that ends up packaged as part of the
 * master distribution (all integrations combined).
 * <p>
 * Note: This is only for defining settings/tasks that have a root/subproject relationship that have to
 * do with packaging. Tasks or settings that are subject to ALL projects (not just the ones that are shipped
 * in the master project archive) should be configured in {@link BuildPlugin}.
 *
 * @see RootBuildPlugin
 */
class IntegrationBuildPlugin implements Plugin<Project> {

    @Override
    void apply(Project target) {
        // Ensure build commands/settings/tasks are on the root project before starting.
        target.getRootProject().getPluginManager().apply(RootBuildPlugin.class)

        // Ensure that the common build plugin is already applied
        target.getPluginManager().apply(BuildPlugin.class)

        configureProjectZip(target)
        configureRootProjectDependencies(target)
    }

    /**
     * Add this module's jar output to the root project's master zip distribution.
     * @param project to be configured
     */
    private static void configureProjectZip(Project project) {
        // We do this after evaluation since the scala projects may change around what the final archive name is.
        // TODO: Swap this out with exposing those jars as artifacts to be consumed in a dist project.
        project.afterEvaluate {
            Zip rootDistZip = project.rootProject.getTasks().getByName('distZip') as Zip
            rootDistZip.dependsOn(project.getTasks().pack)

            project.getTasks().withType(Jar.class) { Jar jarTask ->
                // Add jar output under the dist directory
                if (jarTask.name != "itestJar") {
                    rootDistZip.from(jarTask.archiveFile) { CopySpec copySpecification ->
                        copySpecification.into("${project.rootProject.ext.folderName}/dist")
                        copySpecification.setDuplicatesStrategy(DuplicatesStrategy.WARN)
                    }
                }
            }
        }
    }

    /**
     * For all the dependencies set on this project, set them on the root project as well.
     * @param project to be configured
     */
    private static void configureRootProjectDependencies(Project project) {
        project.getConfigurations().getByName('api').getAllDependencies()
                .withType(ExternalDependency.class) { Dependency dependency ->
                    // Set API dependencies as implementation in the uberjar so that not everything is compile scope
                    project.rootProject.getDependencies().add('implementation', dependency)
                }

        project.getConfigurations().getByName('implementation').getAllDependencies()
                .withType(ExternalDependency.class) { Dependency dependency ->
                    project.rootProject.getDependencies().add('implementation', dependency)
                }
    }
}
