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
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.bundling.Zip;

/**
 * Registers the {@code zipDraSnapshotMavenAggregation} task, which produces a
 * DRA-shaped copy of the Central Portal aggregation zip built by
 * {@code com.gradleup.nmcp.aggregation}.
 *
 * <p>The applying project is expected to have {@code com.gradleup.nmcp.aggregation}
 * applied so the upstream {@code zipAggregation} task exists. This plugin
 * intentionally does not touch {@code zipAggregation}; that task's output must
 * remain Sonatype Central Portal compliant.
 *
 * <p>See {@link PrepareDraSnapshotMavenAggregation} for the details of the rewrite.
 */
public class DraMavenAggregationPlugin implements Plugin<Project> {

    @Override
    public void apply(Project project) {
        Provider<String> version = project.provider(() -> project.getVersion().toString());

        TaskProvider<PrepareDraSnapshotMavenAggregation> prepare = project.getTasks().register(
            "prepareDraSnapshotMavenAggregation",
            PrepareDraSnapshotMavenAggregation.class,
            task -> {
                task.setGroup("dra");
                task.setDescription(
                    "Extracts the maven aggregation zip into the DRA snapshot layout: "
                        + "renames Maven-timestamped snapshot filenames back to -SNAPSHOT "
                        + "and generates per-version maven-metadata.xml."
                );
                task.getSourceZip().set(
                    project.getTasks().named("zipAggregation", Zip.class).flatMap(Zip::getArchiveFile)
                );
                task.getVersion().set(version);
                task.getOutputDir().set(project.getLayout().getBuildDirectory().dir("dra-maven-aggregation"));
            }
        );

        project.getTasks().register("zipDraSnapshotMavenAggregation", Zip.class, zip -> {
            zip.setGroup("dra");
            zip.setDescription(
                "Repackages the maven aggregation zip into the layout expected by "
                    + "DRA snapshot publishing (snapshots.elastic.co / artifacts.elastic.co)."
            );
            zip.getArchiveBaseName().set("elasticsearch-hadoop-dra-maven-aggregation");
            zip.getArchiveVersion().set(version);
            zip.getDestinationDirectory().set(project.getLayout().getBuildDirectory().dir("distributions"));
            zip.from(prepare.flatMap(PrepareDraSnapshotMavenAggregation::getOutputDir));
        });
    }
}
