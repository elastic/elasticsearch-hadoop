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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

/**
 *
 */
public class HdfsUtils {

    public static void copyFromLocal(String localPath) {
        copyFromLocal(localPath, localPath);
    }

    public static void copyFromLocal(String localPath, String destination) {
        try {
            JobConf hadoopConfig = HdpBootstrap.hadoopConfig();
            FileSystem fs = FileSystem.get(hadoopConfig);
            if (!(fs instanceof LocalFileSystem)) {
                Path src = new Path(localPath);
                Path dst = new Path(destination);
                fs.copyFromLocalFile(false, true, src, dst);
                System.out.println(String.format("Copying [%s] to [%s]", src, dst));
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static boolean rmr(Configuration cfg, String path) {
        FileSystem fs;
        try {
            fs = FileSystem.get(cfg);
            return fs.delete(new Path(path), true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String qualify(String path, Configuration config) {
        try {
            return new Path(path).makeQualified(FileSystem.get(config)).toString();
        } catch (IOException e) {
            throw new RuntimeException("Cannot qualify " + path);
        }
    }
}
