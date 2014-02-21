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

import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.util.Progressable;
import org.elasticsearch.hadoop.rest.stats.Stats;

class ReportingUtils {

    // handles Hadoop 'old' and 'new' API reporting classes, namely {@link Reporter} and {@link TaskInputOutputContext}
    @SuppressWarnings({ "rawtypes", "unchecked" })
    static void report(Progressable progressable, Stats stats) {
        if (progressable == null) {
            return;
        }

        if (progressable instanceof Reporter) {
            Reporter reporter = (Reporter) progressable;

            reporter.getCounter(Counters.BYTES_WRITTEN).increment(stats.bytesWritten);
            reporter.getCounter(Counters.BYTES_READ).increment(stats.bytesRead);
            reporter.getCounter(Counters.DOCS_WRITTEN).increment(stats.docsWritten);
            reporter.getCounter(Counters.BULK_WRITES).increment(stats.bulkWrites);
            reporter.getCounter(Counters.DOCS_RETRIED).increment(stats.docsRetried);
            reporter.getCounter(Counters.BULK_RETRIES).increment(stats.bulkRetries);
            reporter.getCounter(Counters.DOCS_READ).increment(stats.docsRead);
            reporter.getCounter(Counters.NODE_RETRIES).increment(stats.nodeRetries);
            reporter.getCounter(Counters.NET_RETRIES).increment(stats.netRetries);
        }

        if (progressable instanceof TaskInputOutputContext) {
            TaskInputOutputContext tioc = (TaskInputOutputContext) progressable;

            tioc.getCounter(Counters.BYTES_WRITTEN).increment(stats.bytesWritten);
            tioc.getCounter(Counters.BYTES_READ).increment(stats.bytesRead);
            tioc.getCounter(Counters.DOCS_WRITTEN).increment(stats.docsWritten);
            tioc.getCounter(Counters.BULK_WRITES).increment(stats.bulkWrites);
            tioc.getCounter(Counters.DOCS_RETRIED).increment(stats.docsRetried);
            tioc.getCounter(Counters.BULK_RETRIES).increment(stats.bulkRetries);
            tioc.getCounter(Counters.DOCS_READ).increment(stats.docsRead);
            tioc.getCounter(Counters.NODE_RETRIES).increment(stats.nodeRetries);
            tioc.getCounter(Counters.NET_RETRIES).increment(stats.netRetries);
        }
    }
}
