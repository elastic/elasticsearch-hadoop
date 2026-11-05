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

package org.elasticsearch.hadoop.gradle;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.attributes.AttributeContainer;
import org.gradle.api.attributes.Usage;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.language.base.plugins.LifecycleBasePlugin;

/**
 * Root-project plugin that verifies the poms of everything aggregated for Maven Central
 * (the {@code nmcpAggregation} configuration) only reference artifacts that are published as well.
 *
 * <p>Producer side: every project using {@code es.hadoop.build} exposes the poms of all its publications
 * through a consumable configuration whose {@link Usage} is {@link #PUBLISHED_POM_USAGE}
 * (see {@code BuildPlugin#configureMaven}). A dedicated usage keeps that variant invisible to java and nmcp
 * consumers, which request different usages. Resolving only poms keeps the check cheap enough for
 * {@code check}: no jars, javadoc or distribution zips are built.
 *
 * <p>The verification runs as part of {@code check} (PR CI), before {@code zipAggregation}
 * (Maven Central) and before {@code prepareDraSnapshotMavenAggregation} (DRA).
 */
public class PublishedPomVerificationPlugin implements Plugin<Project> {

    public static final String PUBLISHED_POM_USAGE = "es-hadoop-published-pom";
    public static final String PUBLISHED_POMS_ELEMENTS = "publishedPomsElements";

    public static void configureAttributes(Project project, AttributeContainer attributes) {
        attributes.attribute(Usage.USAGE_ATTRIBUTE, project.getObjects().named(Usage.class, PUBLISHED_POM_USAGE));
    }

    static final String NMCP_AGGREGATION_PLUGIN = "com.gradleup.nmcp.aggregation";
    static final String NMCP_AGGREGATION_CONFIGURATION = "nmcpAggregation";
    static final String DRA_MAVEN_AGGREGATION_PLUGIN = "es.hadoop.dra-maven-aggregation";

    @Override
    public void apply(Project project) {
        project.getPluginManager().apply(LifecycleBasePlugin.class);

        Configuration publishedPoms = project.getConfigurations().create("publishedPoms", c -> {
            c.setCanBeConsumed(false);
            c.setCanBeResolved(true);
            c.setDescription("Poms of all publications aggregated for Maven Central");
            configureAttributes(project, c.getAttributes());
        });

        TaskProvider<VerifyPublishedPomDependencies> verify = project.getTasks()
            .register("verifyPublishedPomDependencies", VerifyPublishedPomDependencies.class, task -> {
                task.setGroup(LifecycleBasePlugin.VERIFICATION_GROUP);
                task.setDescription("Verifies published poms only depend on artifacts of this build that are published too.");
                task.getPomFiles().from(publishedPoms);
                task.getMarkerFile().set(project.getLayout().getBuildDirectory().file("verify-published-poms/published-artifacts.txt"));
            });

        project.getTasks().named(LifecycleBasePlugin.CHECK_TASK_NAME).configure(t -> t.dependsOn(verify));

        // Reuse the nmcp aggregation dependencies so the verified set can never drift from what is published.
        project.getPluginManager().withPlugin(NMCP_AGGREGATION_PLUGIN, p -> {
            publishedPoms.extendsFrom(project.getConfigurations().getByName(NMCP_AGGREGATION_CONFIGURATION));
            project.getTasks().named("zipAggregation").configure(t -> t.dependsOn(verify));
        });
        // The DRA path reads zipAggregation's sources directly and never runs zipAggregation itself.
        project.getPluginManager().withPlugin(DRA_MAVEN_AGGREGATION_PLUGIN, p ->
            project.getTasks().named("prepareDraSnapshotMavenAggregation").configure(t -> t.dependsOn(verify))
        );
    }
}
