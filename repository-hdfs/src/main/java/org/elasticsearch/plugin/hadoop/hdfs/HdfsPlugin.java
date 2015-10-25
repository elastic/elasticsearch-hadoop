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
package org.elasticsearch.plugin.hadoop.hdfs;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardRepository;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesModule;
import org.elasticsearch.repositories.Repository;

public class HdfsPlugin extends Plugin {

    @Override
    public String name() {
        return "repository-hdfs";
    }

    @Override
    public String description() {
        return "HDFS Snapshot/Restore Plugin";
    }

    @SuppressWarnings("unchecked")
    public void onModule(RepositoriesModule repositoriesModule) {
        String baseLib = detectLibFolder();

        List<URL> cp = new ArrayList<>();
        // add plugin internal jar
        discoverJars(createURI(baseLib, "internal-libs"), cp);
        // add Hadoop jars
        discoverJars(createURI(baseLib, "hadoop-libs"), cp);

        ClassLoader hadoopCL = URLClassLoader.newInstance(cp.toArray(new URL[cp.size()]), getClass().getClassLoader());

        Class<? extends Repository> repository = null;
        try {
            repository = (Class<? extends Repository>) hadoopCL.loadClass("org.elasticsearch.repositories.hdfs.HdfsRepository");
        } catch (ClassNotFoundException cnfe) {
            throw new IllegalStateException("Cannot load plugin class; is the plugin class setup correctly?", cnfe);
        }

        repositoriesModule.registerRepository("hdfs", repository, BlobStoreIndexShardRepository.class);

        Loggers.getLogger(HdfsPlugin.class).info("Loaded Hadoop [{}] libraries from {}", getHadoopVersion(hadoopCL), baseLib);

        if (System.getSecurityManager() != null) {
            Loggers.getLogger(HdfsPlugin.class).warn("The Java Security Manager is enabled however Hadoop is not compatible with it and thus needs to be disabled; see the docs for more information...");
        }
    }

    private String getHadoopVersion(ClassLoader hadoopCL) {
        String version = "Unknown";

        Class<?> clz = null;
        try {
            clz = hadoopCL.loadClass("org.apache.hadoop.util.VersionInfo");
        } catch (ClassNotFoundException cnfe) {
            // unknown
        }

        if (clz != null) {
            try {
                Method method = clz.getMethod("getVersion");
                version = method.invoke(null).toString();
            } catch (Exception ex) {
                // class has changed, ignore
            }
        }

        return version;
    }

    private String detectLibFolder() {
        ClassLoader cl = getClass().getClassLoader();

        // we could get the URL from the URLClassloader directly
        // but that can create issues when running the tests from the IDE
        // we could detect that by loading resources but that as well relies on the JAR URL
        String classToLookFor = getClass().getName().replace(".", "/").concat(".class");
        URL classURL = cl.getResource(classToLookFor);
        if (classURL == null) {
            throw new IllegalStateException("Cannot detect itself; something is wrong with this ClassLoader " + cl);
        }

        String base = classURL.toString();

        // extract root
        // typically a JAR URL
        int index = base.indexOf("!/");
        if (index > 0) {
            base = base.substring(0, index);
            // remove its prefix (jar:)
            base = base.substring(4);
            // remove the trailing jar
            index = base.lastIndexOf("/");
            base = base.substring(0, index + 1);
        }
        // not a jar - something else, do a best effort here
        else {
            // remove the class searched
            base = base.substring(0, base.length() - classToLookFor.length());
        }

        // append hadoop-libs/
        if (!base.endsWith("/")) {
            base = base.concat("/");
        }

        return base;
    }

    private URI createURI(String base, String suffix) {
        String location = base + suffix;
        try {
            return new URI(location);
        } catch (URISyntaxException ex) {
            throw new IllegalStateException(String.format("Cannot detect plugin folder; [%s] seems invalid", location), ex);
        }
    }

    private void discoverJars(URI libPath, List<URL> cp) {
        try {
            Path[] jars = FileSystemUtils.files(Paths.get(libPath), "*.jar");

            for (Path path : jars) {
                cp.add(path.toUri().toURL());
            }
        } catch (IOException ex) {
            throw new IllegalStateException("Cannot compute plugin classpath", ex);
        }
    }
}