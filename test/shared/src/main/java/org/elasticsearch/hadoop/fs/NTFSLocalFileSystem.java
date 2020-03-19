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
package org.elasticsearch.hadoop.fs;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 * Work-around some of the hiccups around NTFS.
 */
public class NTFSLocalFileSystem extends LocalFileSystem {

    public static final FsPermission WIN_PERMS = FsPermission.createImmutable((short) 650);
    public static final String SCRATCH_DIR = "/tmp/hive";
    public static final FsPermission SCRATCH_DIR_PERMS = new FsPermission((short) 00777);

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

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
        //  ignore permission
        //return super.mkdirs(f, WIN_PERMS);
        return super.mkdirs(f);
    }

    @Override
    public void setPermission(Path p, FsPermission permission) throws IOException {
        // ignore
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        // it's the RawFS in place which messes things up as it dynamically returns the permissions...
        // workaround by doing a copy
        FileStatus fs = super.getFileStatus(f);

        // work-around for Hive 0.14
        if (SCRATCH_DIR.equals(f.toString())) {
            System.out.println("Faking scratch dir permissions on Windows...");

            return new FileStatus(fs.getLen(), fs.isDir(), fs.getReplication(), fs.getBlockSize(),
                    fs.getModificationTime(), fs.getAccessTime(), SCRATCH_DIR_PERMS, fs.getOwner(), fs.getGroup(),
                    fs.getPath());
            // this doesn't work since the RawFS impl has its own algo that does the lookup dynamically
            //fs.getPermission().fromShort((short) 777);
        }
        return fs;
    }
}
