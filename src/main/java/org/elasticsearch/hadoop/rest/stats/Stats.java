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
package org.elasticsearch.hadoop.rest.stats;

import org.elasticsearch.hadoop.rest.RestRepository;

/**
 * Basic class gathering stats within a {@link RestRepository} instance.
 */
public class Stats {

    /** writes */
    public long bytesWritten;
    public long docsWritten;
    public long bulkWrites;
    public long docsRetried;
    public long bytesRetried;
    public long bulkRetries;

    /** reads */
    public long bytesRead;
    public long docsRead;

    public int nodeRetries;
    public int netRetries;

    public Stats aggregate(Stats other) {
        if (other == null) {
            return this;
        }

        bytesWritten += other.bytesWritten;
        docsWritten += other.docsWritten;
        bulkWrites += other.bulkWrites;
        docsRetried += other.docsRetried;
        bytesRetried += other.bytesRetried;
        bulkRetries += other.bulkRetries;

        bytesRead += other.bytesRead;
        docsRead += other.docsRead;

        nodeRetries += other.nodeRetries;
        netRetries += other.netRetries;

        return this;
    }
}
