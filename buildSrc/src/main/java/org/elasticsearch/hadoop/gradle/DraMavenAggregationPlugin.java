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
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.bundling.Zip;

/**
 * Registers the {@code prepareDraSnapshotMavenAggregation} task, which produces a
 * DRA-shaped copy of the Central Portal aggregation built by
 * {@code com.gradleup.nmcp.aggregation}.
 *
 * <p>The applying project is expected to have {@code com.gradleup.nmcp.aggregation}
 * applied so the upstream {@code zipAggregation} task exists. This plugin
 * intentionally does not touch {@code zipAggregation}; that task's output must
 * remain Sonatype Central Portal compliant.
 *
 * <p>The task emits an <em>exploded</em> maven tree under
 * {@code build/dra-maven-aggregation/} rather than a zip: the DRA publish step
 * ({@code .buildkite/dra-maven-publish.sh}) runs inline in the
 * same workspace and uploads the tree straight to S3, so re-zipping here just to
 * unzip it again there would be wasted work.
 *
 * <p>To avoid zipping on the DRA path entirely, the task consumes
 * {@code zipAggregation}'s copy-spec source (the already-extracted per-project
 * publications) instead of the {@code aggregation.zip} archive, so that zip is
 * never built for DRA. See {@link PrepareDraSnapshotMavenAggregation}.
 */
public class DraMavenAggregationPlugin implements Plugin<Project> {

    @Override
    public void apply(Project project) {
        Provider<String> version = project.provider(() -> project.getVersion().toString());

        project.getTasks().register(
            "prepareDraSnapshotMavenAggregation",
            PrepareDraSnapshotMavenAggregation.class,
            task -> {
                task.setGroup("dra");
                task.setDescription(
                    "Copies the maven aggregation content into the DRA snapshot layout: "
                        + "renames Maven-timestamped snapshot filenames back to -SNAPSHOT "
                        + "and generates per-version maven-metadata.xml."
                );
                // Reuse zipAggregation's copy-spec source (the extracted
                // per-project publications) rather than its archive output, so
                // the aggregation zip is never built on the DRA path. The
                // lookup is deferred inside a plain provider (rather than
                // resolved eagerly here, or mapped off the TaskProvider):
                //  - TaskProvider.map would add a dependency on zipAggregation
                //    itself, forcing the zip to build;
                //  - resolving named("zipAggregation") eagerly at apply() time
                //    would couple this plugin to being applied *after*
                //    nmcp.aggregation.
                // getSource()'s FileTree already carries the build dependencies
                // of the underlying publication tasks, so @InputFiles
                // establishes the correct task ordering on its own.
                task.getSource().from(
                    project.provider(() -> project.getTasks().named("zipAggregation", Zip.class).get().getSource())
                );
                task.getVersion().set(version);
                task.getOutputDir().set(project.getLayout().getBuildDirectory().dir("dra-maven-aggregation"));
            }
        );
    }
}
