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
package org.apache.hadoop.util;

// mocking class that mimics Hadoop shutdown manager in Hadoop 2.x on which Spark depends
public class ShutdownHookManager {

    private static final ShutdownHookManager STATIC = new ShutdownHookManager();

    public static ShutdownHookManager get() {
        return STATIC;
    }

    public void addShutdownHook(Runnable shutdownHook, int priority) {
        // sure, whatever you say
    }
}
