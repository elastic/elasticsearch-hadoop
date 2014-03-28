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
    /** ack */
    public long bytesAccepted;
    public long docsAccepted;
    /** reads */
    public long bytesRead;
    public long docsRead;
    /** fall overs */
    public int nodeRetries;
    public int netRetries;
    /** time measured (in millis)*/
    public long netTotalTime;
    public long bulkTotalTime;
    public long bulkRetriesTotalTime;
    /** scroll */
    public long scrollTotalTime;
    public long scrollReads;

    public Stats() {};

    public Stats(Stats stats) {
        if (stats == null) {
            return;
        }

        this.bytesWritten = stats.bytesWritten;
        this.docsWritten = stats.docsWritten;
        this.bulkWrites = stats.bulkWrites;

        this.docsRetried = stats.docsRetried;
        this.bytesRetried = stats.bytesRetried;
        this.bulkRetries = stats.bulkRetries;

        this.bytesAccepted = stats.bytesAccepted;
        this.docsAccepted = stats.docsAccepted;

        this.bytesRead = stats.bytesRead;
        this.docsRead = stats.docsRead;

        this.nodeRetries = stats.nodeRetries;
        this.netRetries = stats.netRetries;

        this.netTotalTime = stats.netTotalTime;
        this.bulkTotalTime = stats.bulkTotalTime;
        this.bulkRetriesTotalTime = stats.bulkRetriesTotalTime;

        this.scrollReads = stats.scrollReads;
        this.scrollTotalTime = stats.scrollTotalTime;
    }

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
        bytesAccepted += other.bytesAccepted;
        docsAccepted += other.docsAccepted;

        bytesRead += other.bytesRead;
        docsRead += other.docsRead;

        nodeRetries += other.nodeRetries;
        netRetries += other.netRetries;

        netTotalTime += other.netTotalTime;
        bulkTotalTime += other.bulkTotalTime;
        bulkRetriesTotalTime += other.bulkRetriesTotalTime;

        scrollReads += other.scrollReads;
        scrollTotalTime += other.scrollTotalTime;

        return this;
    }
}