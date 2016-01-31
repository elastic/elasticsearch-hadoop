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
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.DomainCombiner;
import java.security.PrivilegedAction;
import java.security.ProtectionDomain;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.io.PathUtils;
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
        return "HDFS Repository Plugin";
    }

    @SuppressWarnings("unchecked")
    public void onModule(RepositoriesModule repositoriesModule) {
        if (System.getSecurityManager() != null) {
            Loggers.getLogger(HdfsPlugin.class).warn("The Java Security Manager is enabled however Hadoop is not compatible with it and thus needs to be disabled; see the docs for more information...");
        }

        String baseLib = detectLibFolder();
        List<URL> cp = getHadoopClassLoaderPath(baseLib);

        ClassLoader hadoopCL = URLClassLoader.newInstance(cp.toArray(new URL[cp.size()]), getClass().getClassLoader());

        Class<? extends Repository> repository = null;
        try {
            repository = (Class<? extends Repository>) hadoopCL.loadClass("org.elasticsearch.repositories.hdfs.HdfsRepository");
        } catch (ClassNotFoundException cnfe) {
            throw new IllegalStateException("Cannot load plugin class; is the plugin class setup correctly?", cnfe);
        }

        repositoriesModule.registerRepository("hdfs", repository, BlobStoreIndexShardRepository.class);
        Loggers.getLogger(HdfsPlugin.class).info("Loaded Hadoop [{}] libraries from {}", getHadoopVersion(hadoopCL), baseLib);
    }

    public static AccessControlContext hadoopACC() {
        return AccessController.doPrivileged(new PrivilegedAction<AccessControlContext>() {
            @Override
            public AccessControlContext run() {
                return new AccessControlContext(AccessController.getContext(), new HadoopDomainCombiner());
            }
        });
    }

    private static class HadoopDomainCombiner implements DomainCombiner {

        private static String BASE_LIB = detectLibFolder();

        @Override
        public ProtectionDomain[] combine(ProtectionDomain[] currentDomains, ProtectionDomain[] assignedDomains) {
            for (ProtectionDomain pd : assignedDomains) {
                if (pd.getCodeSource().getLocation().toString().startsWith(BASE_LIB)) {
                    return assignedDomains;
                }
            }

            return currentDomains;
        }
    }

    protected List<URL> getHadoopClassLoaderPath(String baseLib) {
        List<URL> cp = new ArrayList<>();
        // add plugin internal jar
        discoverJars(createURI(baseLib, "internal-libs"), cp, false);
        // add Hadoop jars
        discoverJars(createURI(baseLib, "hadoop-libs"), cp, true);
        return cp;
    }

    private String getHadoopVersion(final ClassLoader hadoopCL) {
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            // unprivileged code such as scripts do not have SpecialPermission
            sm.checkPermission(new SpecialPermission());
        }

        return AccessController.doPrivileged(new PrivilegedAction<String>() {
            @Override
            public String run() {
                // Hadoop 2 relies on TCCL to determine the version
                ClassLoader tccl = Thread.currentThread().getContextClassLoader();
                try {
                    Thread.currentThread().setContextClassLoader(hadoopCL);
                    return doGetHadoopVersion(hadoopCL);
                } finally {
                    Thread.currentThread().setContextClassLoader(tccl);
                }
            }
        }, hadoopACC());
    }

    private String doGetHadoopVersion(ClassLoader hadoopCL) {
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

    private static String detectLibFolder() {
        ClassLoader cl = HdfsPlugin.class.getClassLoader();

        // we could get the URL from the URLClassloader directly
        // but that can create issues when running the tests from the IDE
        // we could detect that by loading resources but that as well relies on
        // the JAR URL
        String classToLookFor = HdfsPlugin.class.getName().replace(".", "/").concat(".class");
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

        // append /
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
            throw new IllegalStateException(String.format(Locale.ROOT, "Cannot detect plugin folder; [%s] seems invalid", location), ex);
        }
    }

    @SuppressForbidden(reason = "discover nested jar")
    private void discoverJars(URI libPath, List<URL> cp, boolean optional) {
        try {
            Path[] jars = FileSystemUtils.files(PathUtils.get(libPath), "*.jar");

            for (Path path : jars) {
                cp.add(path.toUri().toURL());
            }
        } catch (IOException ex) {
            if (!optional) {
                throw new IllegalStateException("Cannot compute plugin classpath", ex);
            }
        }
    }
}