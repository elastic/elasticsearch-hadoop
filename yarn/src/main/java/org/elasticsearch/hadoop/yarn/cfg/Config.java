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
package org.elasticsearch.hadoop.yarn.cfg;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.elasticsearch.hadoop.yarn.util.Assert;
import org.elasticsearch.hadoop.yarn.util.PropertiesUtils;
import org.elasticsearch.hadoop.yarn.util.StringUtils;

public class Config {

    private static final String CLIENT_CFG = "cfg.properties";
    private final Properties cfg;

    public Config(Properties props) {
        this.cfg = new Properties(PropertiesUtils.load(Config.class, CLIENT_CFG));
        PropertiesUtils.merge(cfg, props);
    }

    public String appType() {
        return cfg.getProperty("app.type");
    }

    public String appName() {
        return cfg.getProperty("app.name");
    }

    public int amPriority() {
        return Integer.parseInt(cfg.getProperty("app.priority"));
    }

    public String amQueue() {
        return cfg.getProperty("app.queue");
    }

    public int amMem() {
        return Integer.parseInt(cfg.getProperty("am.mem"));
    }

    public int amVCores() {
        return Integer.parseInt(cfg.getProperty("am.vcores"));
    }

    public Set<String> appTags() {
        return new LinkedHashSet<String>(StringUtils.tokenize(cfg.getProperty("app.tags")));
    }

    private String hdfsUploadDir() {
        String dir = cfg.getProperty("hdfs.upload.dir");
        return dir.endsWith("/") ? dir : dir + "/";
    }

    public String jarHdfsPath() {
        return hdfsUploadDir() + jarName();
    }

    public String jarName() {
        return cfg.getProperty("hdfs.es.yarn.jar");
    }

    public String esZipHdfsPath() {
        return hdfsUploadDir() + esZipName();
    }

    public String esZipName() {
        return "elasticsearch-" + downloadEsVersion() + ".zip";
    }

    public String esScript() {
        // the zip contains a folder
        // include the zip name plus the folder
        return "elasticsearch-" + downloadEsVersion() + "/bin/elasticsearch";
    }

    public int containerPriority() {
        return Integer.parseInt(cfg.getProperty("container.priority"));
    }

    public int containerMem() {
        return Integer.parseInt(cfg.getProperty("container.mem"));
    }

    public int containerVCores() {
        return Integer.parseInt(cfg.getProperty("container.vcores"));
    }

    public int containersToAllocate() {
        return Integer.parseInt(cfg.getProperty("containers"));
    }

    public URL downloadURL() {
        String url = cfg.getProperty("download.es.full.url");
        url = (StringUtils.hasText(url) ? url : cfg.getProperty("download.es.url") + esZipName());
        try {
            return new URL(url);
        } catch (MalformedURLException ex) {
            throw new IllegalArgumentException("Invalid URL", ex);
        }
    }

    public String downloadLocalDir() {
        return cfg.getProperty("download.local.dir");
    }

    public String downloadEsVersion() {
        return cfg.getProperty("es.version");
    }

    public File downloadedEs() {
        String dl = downloadLocalDir();
        if (!dl.endsWith("/")) {
            dl = dl + "/";
        }
        return new File(dl + esZipName());
    }

    public File downloadedEsYarn() {
        // internal property
        String jar = cfg.getProperty("internal.es.yarn.file");

        if (!StringUtils.hasText(jar)) {
            Class<?> clazz = getClass();
            // detect jar
            ClassLoader loader = clazz.getClassLoader();
            String classFile = clazz.getName().replaceAll("\\.", "/") + ".class";
            try {
                for (Enumeration<URL> itr = loader.getResources(classFile); itr.hasMoreElements();) {
                    URL url = itr.nextElement();
                    if ("jar".equals(url.getProtocol())) {
                        String toReturn = url.getPath();
                        if (toReturn.startsWith("file:")) {
                            toReturn = toReturn.substring("file:".length());
                        }
                        toReturn = URLDecoder.decode(toReturn, "UTF-8");
                        jar = toReturn.replaceAll("!.*$", "");
                    }
                }
            } catch (IOException ex) {
                throw new IllegalStateException("Cannot detect the ES-YARN jar", ex);
            }
        }

        Assert.hasText(jar, "Es-YARN.jar is not set and could not be detected...");
        return new File(jar);
    }

    public Map<String, String> envVars() {
        Map<String, String> env = new LinkedHashMap<String, String>();
        Set<String> keys = cfg.stringPropertyNames();
        String prefix = "env.";
        for (String key : keys) {
            if (key.startsWith(prefix)) {
                env.put(key.substring(prefix.length()), cfg.getProperty(key));
            }
        }
        return env;
    }

    public Map<String, String> systemProps() {
        Map<String, String> sysProp = new LinkedHashMap<String, String>();
        Set<String> keys = cfg.stringPropertyNames();
        String prefix = "sys.prop.";
        for (String key : keys) {
            if (key.startsWith(prefix)) {
                sysProp.put(key.substring(prefix.length()), cfg.getProperty(key));
            }
        }
        return sysProp;
    }

    public Properties asProperties() {
        return PropertiesUtils.merge(null, cfg);
    }
}