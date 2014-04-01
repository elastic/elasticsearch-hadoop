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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Work-around some of the hiccups around NTFS.
 */
public class NTFSLocalFileSystem extends LocalFileSystem {

    public NTFSLocalFileSystem() {
        super();
    }

    public NTFSLocalFileSystem(FileSystem rawLocalFileSystem) {
        super(rawLocalFileSystem);
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        super.rename(src, dst);
        // always return true
        return true;
    }
}
