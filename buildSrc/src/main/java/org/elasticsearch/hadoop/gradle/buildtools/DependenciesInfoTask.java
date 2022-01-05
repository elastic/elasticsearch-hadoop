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

import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.Dependency;
import org.gradle.api.artifacts.DependencySet;
import org.gradle.api.artifacts.ModuleVersionIdentifier;
import org.gradle.api.artifacts.ProjectDependency;
import org.gradle.api.internal.ConventionTask;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputDirectory;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.TaskAction;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * A task to gather information about the dependencies and export them into a csv file.
 * <p>
 * The following information is gathered:
 * <ul>
 *     <li>name: name that identifies the library (groupId:artifactId)</li>
 *     <li>version</li>
 *     <li>URL: link to have more information about the dependency.</li>
 *     <li>license: <a href="https://spdx.org/licenses/">SPDX license</a> identifier, custom license or UNKNOWN.</li>
 * </ul>
 */
public class DependenciesInfoTask extends ConventionTask {
    /**
     * Directory to read license files
     */
    @Optional
    @InputDirectory
    private File licensesDir = new File(getProject().getProjectDir(), "licenses").exists()
        ? new File(getProject().getProjectDir(), "licenses")
        : null;

    @OutputFile
    private File outputFile = new File(getProject().getBuildDir(), "reports/dependencies/dependencies.csv");
    private LinkedHashMap<String, String> mappings;

    public Configuration getRuntimeConfiguration() {
        return runtimeConfiguration;
    }

    public void setRuntimeConfiguration(Configuration runtimeConfiguration) {
        this.runtimeConfiguration = runtimeConfiguration;
    }

    public Configuration getCompileOnlyConfiguration() {
        return compileOnlyConfiguration;
    }

    public void setCompileOnlyConfiguration(Configuration compileOnlyConfiguration) {
        this.compileOnlyConfiguration = compileOnlyConfiguration;
    }

    public File getLicensesDir() {
        return licensesDir;
    }

    public void setLicensesDir(File licensesDir) {
        this.licensesDir = licensesDir;
    }

    public File getOutputFile() {
        return outputFile;
    }

    public void setOutputFile(File outputFile) {
        this.outputFile = outputFile;
    }

    /**
     * Dependencies to gather information from.
     */
    @InputFiles
    private Configuration runtimeConfiguration;
    /**
     * We subtract compile-only dependencies.
     */
    @InputFiles
    private Configuration compileOnlyConfiguration;

    public DependenciesInfoTask() {
        setDescription("Create a CSV file with dependencies information.");
    }

    @TaskAction
    public void generateDependenciesInfo() throws IOException {

        final DependencySet runtimeDependencies = runtimeConfiguration.getAllDependencies();
        // we have to resolve the transitive dependencies and create a group:artifactId:version map

        final Set<String> compileOnlyArtifacts = compileOnlyConfiguration.getResolvedConfiguration()
            .getResolvedArtifacts()
            .stream()
            .map(r -> {
                ModuleVersionIdentifier id = r.getModuleVersion().getId();
                return id.getGroup() + ":" + id.getName() + ":" + id.getVersion();
            })
            .collect(Collectors.toSet());

        final StringBuilder output = new StringBuilder();
        for (final Dependency dep : runtimeDependencies) {
            // we do not need compile-only dependencies here
            if (compileOnlyArtifacts.contains(dep.getGroup() + ":" + dep.getName() + ":" + dep.getVersion())) {
                continue;
            }

            // only external dependencies are checked
            if (dep instanceof ProjectDependency) {
                continue;
            }

            final String url = createURL(dep.getGroup(), dep.getName(), dep.getVersion());
            final String dependencyName = DependencyLicensesTask.getDependencyName(getMappings(), dep.getName());
            getLogger().info("mapped dependency " + dep.getGroup() + ":" + dep.getName() + " to " + dependencyName + " for license info");

            final String licenseType = getLicenseType(dep.getGroup(), dependencyName);
            output.append(dep.getGroup() + ":" + dep.getName() + "," + dep.getVersion() + "," + url + "," + licenseType + "\n");
        }

        Files.write(outputFile.toPath(), output.toString().getBytes("UTF-8"), StandardOpenOption.CREATE);
    }

    @Input
    public LinkedHashMap<String, String> getMappings() {
        return mappings;
    }

    public void setMappings(LinkedHashMap<String, String> mappings) {
        this.mappings = mappings;
    }

    /**
     * Create an URL on <a href="https://repo1.maven.org/maven2/">Maven Central</a>
     * based on dependency coordinates.
     */
    protected String createURL(final String group, final String name, final String version) {
        final String baseURL = "https://repo1.maven.org/maven2";
        return baseURL + "/" + group.replaceAll("\\.", "/") + "/" + name + "/" + version;
    }

    /**
     * Read the LICENSE file associated with the dependency and determine a license type.
     * <p>
     * The license type is one of the following values:
     * <ul>
     * <li><em>UNKNOWN</em> if LICENSE file is not present for this dependency.</li>
     * <li><em>one SPDX identifier</em> if the LICENSE content matches with an SPDX license.</li>
     * <li><em>Custom;URL</em> if it's not an SPDX license,
     * URL is the Github URL to the LICENSE file in elasticsearch repository.</li>
     * </ul>
     *
     * @param group dependency group
     * @param name  dependency name
     * @return SPDX identifier, UNKNOWN or a Custom license
     */
    protected String getLicenseType(final String group, final String name) throws IOException {
        final File license = getDependencyInfoFile(group, name, "LICENSE");
        String licenseType;

        final LicenseAnalyzer.LicenseInfo licenseInfo = LicenseAnalyzer.licenseType(license);
        if (licenseInfo.isSpdxLicense() == false) {
            // License has not be identified as SPDX.
            // As we have the license file, we create a Custom entry with the URL to this license file.
            final String gitBranch = System.getProperty("build.branch", "master");
            final String githubBaseURL = "https://raw.githubusercontent.com/elastic/elasticsearch/" + gitBranch + "/";
            licenseType = licenseInfo.getIdentifier()
                + ";"
                + license.getCanonicalPath().replaceFirst(".*/elasticsearch/", githubBaseURL)
                + ",";
        } else {
            licenseType = licenseInfo.getIdentifier() + ",";
        }

        if (licenseInfo.isSourceRedistributionRequired()) {
            final File sources = getDependencyInfoFile(group, name, "SOURCES");
            licenseType += Files.readString(sources.toPath()).trim();
        }

        return licenseType;
    }

    protected File getDependencyInfoFile(final String group, final String name, final String infoFileSuffix) {
        java.util.Optional<File> license = licensesDir != null
            ? Arrays.stream(licensesDir.listFiles((dir, fileName) -> Pattern.matches(".*-" + infoFileSuffix + ".*", fileName)))
                .filter(file -> {
                    String prefix = file.getName().split("-" + infoFileSuffix + ".*")[0];
                    return group.contains(prefix) || name.contains(prefix);
                })
                .findFirst()
            : java.util.Optional.empty();

        return license.orElseThrow(
            () -> new IllegalStateException(
                "Unable to find "
                    + infoFileSuffix
                    + " file for dependency "
                    + group
                    + ":"
                    + name
                    + " in "
                    + getLicensesDir().getAbsolutePath()
            )
        );
    }
}
