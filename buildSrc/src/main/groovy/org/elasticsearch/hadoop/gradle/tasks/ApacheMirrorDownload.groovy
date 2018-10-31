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
 *
 * Note: Apache's download mirrors are almost always http only. Since we cannot rely on downloading
 * over https, you should always use the {@linkplain org.elasticsearch.hadoop.gradle.tasks.VerifyChecksums}
 * task to make sure the package you are downloading is safe/intact.
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
    private String artifactFileName
    private File downloadDir = project.rootProject.buildDir.toPath().resolve("downloadcache").toFile()

    /**
     * @return The path on the apache download server to the directory that holds the versioned artifact directories
     */
    String getPackagePath() {
        return packagePath
    }

    /**
     * Sets the path to the directory on the Apache download server that holds the versioned artifact directories.
     * Also used to look up a valid mirror server for downloading the artifact for this task.
     * @param packagePath The path on the apache download server to the directory that holds the versioned artifact
     * directories
     */
    void setPackagePath(String packagePath) {
        this.packagePath = packagePath
    }

    /**
     * @return The root name of the package to download
     */
    String getPackageName() {
        return packageName
    }

    /**
     * Set the root name of the package to download. The version is appended to this to locate the versioned directory
     * to pick out the actual download artifact, as well as to cache the artifact on the disk once it is downloaded.
     * @param packageName The root name of the package to download
     */
    void setPackageName(String packageName) {
        this.packageName = packageName
    }

    /**
     * @return The version of the package to download
     */
    String getVersion() {
        return version
    }

    /**
     * @param version The version of the package to download
     */
    void setVersion(String version) {
        this.version = version
    }

    /**
     * @return The final name of the artifact to download, including version and file extension
     */
    String getArtifactFileName() {
        return artifactFileName
    }

    /**
     * @param artifactFileName The final name of the artifact to download, including the version and file extension
     */
    void setArtifactFileName(String artifactFileName) {
        this.artifactFileName = artifactFileName
    }

    /**
     * @return The local directory that this task will download its artifact into
     */
    @OutputDirectory
    File getArtifactDirectory() {
        return downloadDir.toPath().resolve("${packageName}-${version}").toFile()
    }

    /**
     * @return The downloaded artifact
     */
    File outputFile() {
        return getArtifactDirectory().toPath().resolve(artifactFileName).toFile()
    }

    @TaskAction
    def doMirroredDownload() {
        getArtifactDirectory().mkdirs()
        String packageDirectory = packageName + '-' + version

        MirrorInfo mirrorInfo = getMirrors(packagePath)
        def mirrors = new LinkedList<String>(mirrorInfo.mirrors)
        String mirror = mirrorInfo.preferred
        // Sanitize mirror link
        if (mirror.endsWith('/')) {
            mirror = mirror.substring(0, mirror.length() - 1)
        }
        while (true) {
            // Ex: [http://blah.blah/dist]/[hive]/[hive-1.2.2]/[apache-hive-1.2.2-bin.tar.gz]
            // Ex: [http://blah.blah/dist]/[hadoop/common]/[hadoop-2.7.7]/[hadoop-2.7.7.tar.gz]
            String url = "${mirror}/${packagePath}/${packageDirectory}/${artifactFileName}"
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
