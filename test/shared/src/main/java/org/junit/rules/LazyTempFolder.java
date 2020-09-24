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

package org.junit.rules;

import java.io.File;
import java.io.IOException;

/**
 * A TemporaryFolder rule that creates the temporary directory when it is first requested,
 * but still allows for it to clean up afterward.
 */
public class LazyTempFolder extends TemporaryFolder {

    private volatile boolean created;

    @Override
    protected void before() throws Throwable {
        // Do nothing. Only create the temp folder if it was requested.
    }

    @Override
    protected void after() {
        if (created) {
            super.after();
        }
    }

    public File getOrCreateFolder(String name) {
        File root = getRoot();
        File folder = new File(root, name);
        if (folder.exists()) {
            if (!folder.isDirectory()) {
                throw new IllegalStateException("Cannot get or create temp folder [" + name + "] as it is already a file");
            }
        } else {
            try {
                folder = super.newFolder(name);
            } catch (IOException e) {
                throw new IllegalStateException("Cannot create temp folder [" + name + "]", e);
            }
        }
        return folder;
    }

    @Override
    public File getRoot() {
        if (created == false) {
            synchronized (this) {
                if (created == false) {
                    try {
                        super.create();
                        // Make sure we delete it on exit in case the after method is missed.
                        File root = super.getRoot();
                        root.deleteOnExit();
                    } catch (IOException e) {
                        throw new IllegalStateException("Could not lazily create temp folder", e);
                    }
                    created = true;
                }
            }
        }
        return super.getRoot();
    }
}
