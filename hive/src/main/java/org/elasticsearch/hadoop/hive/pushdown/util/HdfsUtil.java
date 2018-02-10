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
package org.elasticsearch.hadoop.hive.pushdown.util;


import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;

/**
 * the util functions for Hdfs
 */
public class HdfsUtil {

    /**
     * load hdfs path and return file content
     *
     * @param hdfsPath
     * @param conf
     * @return
     * @throws IOException
     */
    public static String getFileContent(String hdfsPath, Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);

        InputStream in = null;
        try {
            //1.open hdfs file stream
            //2.transform stream to string.
            in = fs.open(new Path(hdfsPath));
            return IOUtils.toString(in);
        } finally {
            if (in != null) {
                in.close();
            }
        }
    }
}
