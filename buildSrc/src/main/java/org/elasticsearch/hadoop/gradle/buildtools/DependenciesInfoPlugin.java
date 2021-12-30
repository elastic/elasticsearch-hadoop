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

package org.elasticsearch.hadoop.gradle.buildtools;

import org.elasticsearch.gradle.dependencies.CompileOnlyResolvePlugin;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.plugins.JavaPlugin;

public class DependenciesInfoPlugin implements Plugin<Project> {
    @Override
    public void apply(final Project project) {
        project.getPlugins().apply(CompileOnlyResolvePlugin.class);
        var depsInfo = project.getTasks().register("dependenciesInfo", DependenciesInfoTask.class);
        depsInfo.configure(t -> {
            t.setRuntimeConfiguration(project.getConfigurations().getByName(JavaPlugin.RUNTIME_CLASSPATH_CONFIGURATION_NAME));
            t.setCompileOnlyConfiguration(
                project.getConfigurations().getByName(CompileOnlyResolvePlugin.RESOLVEABLE_COMPILE_ONLY_CONFIGURATION_NAME)
            );
            t.getConventionMapping().map("mappings", () -> {
                var depLic = project.getTasks().named("dependencyLicenses", DependencyLicensesTask.class);
                return depLic.get().getMappings();
            });
        });
    }

}
