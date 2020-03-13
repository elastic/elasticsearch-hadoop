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
package org.elasticsearch.hadoop.mr;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;

public abstract class HadoopIOUtils {

    private static Log log = LogFactory.getLog(HadoopIOUtils.class);

    public static InputStream open(String resource, Configuration conf) {
        ClassLoader loader = conf.getClassLoader();

        if (loader == null) {
            loader = Thread.currentThread().getContextClassLoader();
        }

        if (loader == null) {
            loader = HadoopIOUtils.class.getClassLoader();
        }

        boolean trace = log.isTraceEnabled();

        try {
            // no prefix means classpath
            if (!resource.contains(":")) {

                InputStream result = loader.getResourceAsStream(resource);
                if (result != null) {
                    if (trace) {
                        log.trace(String.format("Loaded resource %s from classpath", resource));
                    }
                    return result;
                }
                // fall back to the distributed cache
                URI[] uris = DistributedCache.getCacheFiles(conf);
                if (uris != null) {
                    for (URI uri : uris) {
                        if (uri.toString().contains(resource)) {
                            if (trace) {
                                log.trace(String.format("Loaded resource %s from distributed cache", resource));
                            }
                            return uri.toURL().openStream();
                        }
                    }
                }
            }

            // fall back to file system
            Path p = new Path(resource);
            FileSystem fs = p.getFileSystem(conf);
            return fs.open(p);
        } catch (IOException ex) {
            throw new EsHadoopIllegalArgumentException(String.format("Cannot open stream for resource %s", resource));
        }
    }
}
