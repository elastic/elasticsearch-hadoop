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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import javax.xml.parsers.DocumentBuilderFactory;

import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

/**
 * Regression guard for elastic/elasticsearch-hadoop#2519.
 *
 * <p>Takes the poms of every publication that is shipped to Maven Central and fails if any of them
 * declares a dependency on an artifact <em>from this same build</em> (same groupId and version) that is
 * not itself one of the shipped publications. Such a dependency is unresolvable for every consumer,
 * typically because an embedded/shaded project dependency (e.g. {@code thirdparty},
 * {@code elasticsearch-spark_2.12}) leaked into the pom.
 *
 * <p>The set of "published" artifacts is derived from the poms themselves, so this needs no
 * cross-project model access.
 */
@CacheableTask
public abstract class VerifyPublishedPomDependencies extends DefaultTask {

    @InputFiles
    @PathSensitive(PathSensitivity.NONE)
    public abstract ConfigurableFileCollection getPomFiles();

    @OutputFile
    public abstract RegularFileProperty getMarkerFile();

    @TaskAction
    public void verify() throws Exception {
        Set<String> published = new TreeSet<>();
        Map<String, List<Coordinates>> dependenciesByPom = new TreeMap<>();

        for (File pomFile : getPomFiles().getFiles()) {
            Element project = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(pomFile).getDocumentElement();
            Coordinates self = Coordinates.of(project);
            published.add(self.toString());
            List<Coordinates> dependencies = new ArrayList<>();
            Element dependenciesElement = child(project, "dependencies");
            if (dependenciesElement != null) {
                for (Node n = dependenciesElement.getFirstChild(); n != null; n = n.getNextSibling()) {
                    if (n instanceof Element && "dependency".equals(n.getNodeName())) {
                        Coordinates dependency = Coordinates.of((Element) n);
                        // Only dependencies produced by this very build are in scope.
                        if (self.groupId.equals(dependency.groupId) && self.version.equals(dependency.version)) {
                            dependencies.add(dependency);
                        }
                    }
                }
            }
            dependenciesByPom.put(self.toString(), dependencies);
        }

        if (published.isEmpty()) {
            throw new GradleException("No poms found to verify. Is the published-poms variant wired correctly?");
        }

        List<String> problems = new ArrayList<>();
        dependenciesByPom.forEach((pom, dependencies) -> {
            for (Coordinates dependency : dependencies) {
                if (published.contains(dependency.toString()) == false) {
                    problems.add("  " + pom + " -> " + dependency + " (not published)");
                }
            }
        });
        if (problems.isEmpty() == false) {
            throw new GradleException(
                "Published poms declare dependencies on artifacts of this build that are not published to Maven Central.\n"
                    + "Consumers will fail to resolve them. Embedded/shaded project dependencies must be stripped from the pom "
                    + "(see BuildPlugin#configurePom).\n"
                    + String.join("\n", problems)
            );
        }

        File marker = getMarkerFile().get().getAsFile();
        Files.createDirectories(marker.getParentFile().toPath());
        Files.write(marker.toPath(), String.join("\n", published).getBytes(StandardCharsets.UTF_8));
    }

    private static Element child(Element parent, String name) {
        for (Node n = parent.getFirstChild(); n != null; n = n.getNextSibling()) {
            if (n instanceof Element && name.equals(n.getNodeName())) {
                return (Element) n;
            }
        }
        return null;
    }

    private static final class Coordinates {
        final String groupId;
        final String artifactId;
        final String version;

        private Coordinates(String groupId, String artifactId, String version) {
            this.groupId = groupId;
            this.artifactId = artifactId;
            this.version = version;
        }

        static Coordinates of(Element element) throws IOException {
            return new Coordinates(text(element, "groupId"), text(element, "artifactId"), text(element, "version"));
        }

        private static String text(Element element, String name) throws IOException {
            Element child = child(element, name);
            if (child == null) {
                throw new IOException("Missing <" + name + "> in <" + element.getNodeName() + ">");
            }
            return child.getTextContent().trim();
        }

        @Override
        public String toString() {
            return groupId + ":" + artifactId + ":" + version;
        }
    }
}
