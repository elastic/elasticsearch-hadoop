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

package org.elasticsearch.hadoop.gradle.tasks

import groovy.json.JsonSlurper
import org.gradle.api.DefaultTask
import org.gradle.api.tasks.OutputDirectory
import org.gradle.api.tasks.TaskAction

/**
 * Downloads an artifact from the closest selected Apache Download Mirror.
 *
 * Mirror selection is done by requesting the closest mirror from the apache site. The download
 * is performed on the preferred mirror, with falling back on to the other mirrors in the mirror
 * list.
 */
class ApacheMirrorDownload extends DefaultTask {

    private static final String MIRROR_DISCOVERY = "https://www.apache.org/dyn/closer.cgi"
    private static final String JSON_SUFFIX = "?as_json=1"

    static class MirrorInfo {
        String preferred
        List<String> mirrors
        List<String> backups

        MirrorInfo(String mirrorInfo) {
            Object parsed = new JsonSlurper().parseText(mirrorInfo)
            preferred = parsed.preferred
            mirrors = parsed.http
            backups = parsed.backup
            Collections.shuffle(mirrors)
            Collections.shuffle(backups)
        }
    }

    private String packagePath
    private String packageName
    private String version
    private String distribution
    private File downloadDir = project.rootProject.buildDir.toPath().resolve("downloadcache").toFile()

    String getPackagePath() {
        return packagePath
    }

    void setPackagePath(String packagePath) {
        this.packagePath = packagePath
    }

    String getPackageName() {
        return packageName
    }

    void setPackageName(String packageName) {
        this.packageName = packageName
    }

    String getVersion() {
        return version
    }

    void setVersion(String version) {
        this.version = version
    }

    String getDistribution() {
        return distribution
    }

    void setDistribution(String distribution) {
        this.distribution = distribution
    }

    @OutputDirectory
    File getArtifactDirectory() {
        return downloadDir.toPath().resolve("${packageName}-${version}").toFile()
    }

    File outputFile() {
        return getArtifactDirectory().toPath().resolve(packageName + '-' + version + "." + distribution).toFile()
    }

    @TaskAction
    def doMirroredDownload() {
        getArtifactDirectory().mkdirs()
        String artifact = packageName + '-' + version

        MirrorInfo mirrorInfo = getMirrors(packagePath)
        def mirrors = new LinkedList<String>(mirrorInfo.mirrors)
        String mirror = mirrorInfo.preferred
        while (true) {
            String url = "${mirror}${packagePath}/${artifact}/${artifact}.${distribution}"
            try {
                logger.info("Downloading [$url]...")
                project.getAnt().get(
                        src: url,
                        dest: outputFile(),
                        maxtime: (5 * 60).toString() // 5 minutes for download timeout
                )
                break
            } catch (Exception e) {
                if (mirrors.isEmpty()) {
                    throw e
                }
                logger.warn("Could not download [$url]. Trying next mirror.")
                mirror = mirrors.poll()
            }
        }
    }

    private static MirrorInfo getMirrors(String path) {
        String mirrorDiscovery = MIRROR_DISCOVERY
        if (path != null) {
            mirrorDiscovery = mirrorDiscovery + "/" + path
        }
        mirrorDiscovery = mirrorDiscovery + JSON_SUFFIX
        return new MirrorInfo(mirrorDiscovery.toURL().getText())
    }
}
