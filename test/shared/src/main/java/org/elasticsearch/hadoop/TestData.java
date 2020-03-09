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

package org.elasticsearch.hadoop;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.URI;

import com.google.common.io.ByteStreams;
import org.apache.hadoop.conf.Configuration;
import org.elasticsearch.hadoop.mr.HadoopCfgUtils;
import org.junit.rules.LazyTempFolder;

/**
 * Utility for loading and unpacking test data out of jars and into a temporary folders.
 *
 * Many of the technologies in the Hadoop Ecosystem require test data to exist on the filesystem as a plain file.
 * Since we keep our test data in the shared project, it is located within a jar file. This utility can be used
 * to unpack and make those datasets available as regular files.
 */
public class TestData extends LazyTempFolder {

    public static final String DATA_JOIN_DATA_ALL_DAT = "/data/join/data/all.dat";
    public static final String ARTISTS_DAT = "/artists.dat";
    public static final String ARTISTS_JSON = "/artists.json";
    public static final String GIBBERISH_DAT = "/gibberish.dat";
    public static final String GIBBERISH_JSON = "/gibberish.json";

    private File dataRoot;

    private File getDataRoot() throws IOException {
        if (dataRoot == null) {
            dataRoot = getOrCreateFolder("data");
        }
        return dataRoot;
    }

    public URI sampleArtistsJsonUri() throws IOException {
        return sampleArtistsJsonFile().toURI();
    }

    public File sampleArtistsJsonFile() throws IOException {
        return unpackResource(ARTISTS_JSON, getDataRoot());
    }

    public String sampleArtistsJson(Configuration cfg) throws IOException {
        return (HadoopCfgUtils.isLocal(cfg) ? sampleArtistsJsonUri().toString() : ARTISTS_JSON);
    }

    public URI sampleArtistsDatUri() throws IOException {
        return sampleArtistsDatFile().toURI();
    }

    public File sampleArtistsDatFile() throws IOException {
        return unpackResource(ARTISTS_DAT, getDataRoot());
    }

    public String sampleArtistsDat(Configuration cfg) throws IOException {
        return (HadoopCfgUtils.isLocal(cfg) ? sampleArtistsDatUri().toString() : ARTISTS_DAT);
    }

    public URI gibberishDatUri() throws IOException {
        return gibberishDatFile().toURI();
    }

    public File gibberishDatFile() throws IOException {
        return unpackResource(GIBBERISH_DAT, getDataRoot());
    }

    public String gibberishDat(Configuration cfg) throws IOException {
        return (HadoopCfgUtils.isLocal(cfg) ? gibberishDatUri().toString() : GIBBERISH_DAT);
    }

    public URI giberrishJsonUri() throws IOException {
        return gibberishJsonFile().toURI();
    }

    public File gibberishJsonFile() throws IOException {
        return unpackResource(GIBBERISH_JSON, getDataRoot());
    }

    public String gibberishJson(Configuration cfg) throws IOException {
        return (HadoopCfgUtils.isLocal(cfg) ? giberrishJsonUri().toString() : GIBBERISH_JSON);
    }

    public URI sampleJoinDatURI() throws IOException {
        return sampleJoinDatFile().toURI();
    }

    public File sampleJoinDatFile() throws IOException {
        return unpackResource(DATA_JOIN_DATA_ALL_DAT, getDataRoot());
    }

    public static synchronized File unpackResource(String resource, File stagingLocation) {
        if (stagingLocation.exists() == false) {
            throw new IllegalArgumentException("staging location must exist for resource to be unpacked");
        }
        if (stagingLocation.isDirectory() == false) {
            throw new IllegalArgumentException("staging location must be a directory for resource to be unpacked");
        }
        File resourceFile = new File(stagingLocation, resource);
        if (resourceFile.exists()) {
            return resourceFile;
        } else {
            resourceFile.getParentFile().mkdirs();
            if (resourceFile.getParentFile().exists() == false) {
                throw new UncheckedIOException(new IOException("Resource [" + resource + "]: Could not create directory path [" +
                        resourceFile.getParentFile().getAbsolutePath() + "]"));
            }
            try (InputStream in = TestData.class.getResourceAsStream(resource); OutputStream out = new FileOutputStream(resourceFile)) {
                ByteStreams.copy(in, out);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return resourceFile;
        }
    }
}
