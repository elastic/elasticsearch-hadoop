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

import org.gradle.api.DefaultTask;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.file.FileSystemOperations;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.IgnoreEmptyDirectories;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.OutputDirectory;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.stream.Stream;

import javax.inject.Inject;

/**
 * Repackages the Maven Central compliant aggregation zip produced by
 * {@code com.gradleup.nmcp.aggregation} into the layout the DRA snapshot repo
 * ({@code snapshots.elastic.co/maven/}) expects.
 *
 * <p>Rather than depend on the {@code aggregation.zip} archive and unpack it,
 * this task reuses {@code zipAggregation}'s copy-spec source directly (the
 * already-extracted per-project publications). That avoids materializing the
 * DRA-side zip only to unzip it again in the publish step — nothing is zipped
 * on the DRA path at all.
 *
 * <p>For snapshot versions this task:
 * <ol>
 *   <li>Sync-copies the aggregation source into an output directory, renaming
 *       {@code -<yyyyMMdd.HHmmss>-<n>} segments to {@code -SNAPSHOT}.
 *       Per-file checksum sidecars (.md5/.sha1/.sha*) hash the file bytes so
 *       renaming them alongside their jar/pom is byte-safe.</li>
 *   <li>Emits a minimal {@code maven-metadata.xml} per version directory
 *       (with {@code <snapshot><localCopy>true</localCopy></snapshot>}), plus
 *       checksum sidecars, so Gradle/Maven consumers can resolve
 *       {@code <version>-SNAPSHOT} against the literal filenames.</li>
 * </ol>
 *
 * <p>For non-snapshot (release) versions the extract is a plain sync and no
 * metadata is generated.
 *
 * <p>See <a href="https://github.com/elastic/elasticsearch-team/issues/4297">
 * elasticsearch-team#4297</a>.
 */
public abstract class PrepareDraSnapshotMavenAggregation extends DefaultTask {

    // Match the timestamp + build-number segment that maven-publish emits for
    // snapshot deploys, e.g. `-20260824.075015-1`. The trailing `\d+` is
    // digit-only so classifier suffixes like `-sources` / `-javadoc` are
    // preserved by the rename.
    private static final String TIMESTAMP_REGEX = "-\\d{8}\\.\\d{6}-\\d+";

    /**
     * The already-extracted maven aggregation content, wired from
     * {@code zipAggregation}'s copy-spec source so the DRA path never builds
     * (or unpacks) the aggregation zip.
     */
    @InputFiles
    @PathSensitive(PathSensitivity.RELATIVE)
    @IgnoreEmptyDirectories
    public abstract ConfigurableFileCollection getSource();

    @Input
    public abstract Property<String> getVersion();

    @OutputDirectory
    public abstract DirectoryProperty getOutputDir();

    @Inject
    protected abstract FileSystemOperations getFileSystemOperations();

    @TaskAction
    public void prepare() throws IOException {
        String version = getVersion().get();
        boolean snapshot = version.endsWith("-SNAPSHOT");
        File outDir = getOutputDir().get().getAsFile();

        getFileSystemOperations().sync(spec -> {
            spec.from(getSource());
            spec.into(outDir);
            if (snapshot) {
                spec.rename(TIMESTAMP_REGEX, "-SNAPSHOT");
            }
        });

        if (snapshot == false) {
            // Release (staging) path: plain sync with no rename or metadata.
            // Artifact-level maven-metadata.xml is intentionally omitted: DRA
            // consumers always resolve pinned coordinates and never need version
            // discovery from the repo index.
            return;
        }

        String lastUpdated = ZonedDateTime.now(ZoneOffset.UTC)
            .format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
        Path root = outDir.toPath();
        // Collect the version directories eagerly before writing anything: we
        // mutate each directory (adding maven-metadata.xml + sidecars) and must
        // not do so while the lazy Files.walk directory stream is still open.
        List<Path> versionDirs;
        try (Stream<Path> stream = Files.walk(root)) {
            versionDirs = stream.filter(Files::isDirectory)
                .filter(PrepareDraSnapshotMavenAggregation::isVersionDirectory)
                .toList();
        }
        versionDirs.forEach(versionDir -> writeSnapshotMetadata(root, versionDir, lastUpdated));
    }

    private static boolean isVersionDirectory(Path dir) {
        // A version directory is `<groupPath>/<artifactId>/<version>/` and by
        // convention contains at least one `.pom`. Using the pom presence as
        // the marker avoids parsing filenames.
        try (Stream<Path> s = Files.list(dir)) {
            return s.anyMatch(p -> p.getFileName().toString().endsWith(".pom"));
        } catch (IOException e) {
            return false;
        }
    }

    private static void writeSnapshotMetadata(Path root, Path versionDir, String lastUpdated) {
        Path artifactDir = versionDir.getParent();
        Path groupDir = artifactDir.getParent();

        String version = versionDir.getFileName().toString();
        String artifactId = artifactDir.getFileName().toString();

        StringBuilder groupBuilder = new StringBuilder();
        for (Path segment : root.relativize(groupDir)) {
            if (groupBuilder.length() > 0) {
                groupBuilder.append('.');
            }
            groupBuilder.append(segment.toString());
        }
        String groupId = groupBuilder.toString();

        // <localCopy>true</localCopy> tells Maven/Gradle to resolve the literal
        // "-SNAPSHOT" filename rather than looking for a timestamped version.
        // This is correct here because dra-maven-publish.sh uploads files
        // with the -SNAPSHOT suffix (not -yyyyMMdd.HHmmss-N).
        String xml = """
            <?xml version="1.0" encoding="UTF-8"?>
            <metadata>
              <groupId>%s</groupId>
              <artifactId>%s</artifactId>
              <version>%s</version>
              <versioning>
                <snapshot>
                  <localCopy>true</localCopy>
                </snapshot>
                <lastUpdated>%s</lastUpdated>
              </versioning>
            </metadata>
            """.formatted(groupId, artifactId, version, lastUpdated);

        try {
            Path metadata = versionDir.resolve("maven-metadata.xml");
            Files.writeString(metadata, xml, StandardCharsets.UTF_8);
            writeChecksumSidecars(metadata);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void writeChecksumSidecars(Path file) throws IOException {
        byte[] bytes = Files.readAllBytes(file);
        // Match the sidecar set nmcp already emits for the other artifacts in
        // the zip; keeping them symmetric avoids surprise on the S3 side.
        for (String algorithm : new String[] { "MD5", "SHA-1", "SHA-256", "SHA-512" }) {
            try {
                MessageDigest digest = MessageDigest.getInstance(algorithm);
                String hex = HexFormat.of().formatHex(digest.digest(bytes));
                String extension = "." + algorithm.toLowerCase(Locale.ROOT).replace("-", "");
                Files.writeString(file.resolveSibling(file.getFileName() + extension), hex, StandardCharsets.UTF_8);
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException("Missing required digest " + algorithm, e);
            }
        }
    }
}
