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

import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.util.Progressable;
import org.elasticsearch.hadoop.rest.stats.Stats;

class ReportingUtils {

    // handles Hadoop 'old' and 'new' API reporting classes, namely {@link Reporter} and {@link TaskInputOutputContext}
    @SuppressWarnings({ "rawtypes" })
    static void report(Progressable progressable, Stats stats) {
        if (progressable == null || progressable == Reporter.NULL) {
            return;
        }

        if (progressable instanceof Reporter) {
            Reporter reporter = (Reporter) progressable;

            oldApiCounter(reporter, Counters.BYTES_WRITTEN, stats.bytesWritten);
            oldApiCounter(reporter, Counters.BYTES_READ, stats.bytesRead);
            oldApiCounter(reporter, Counters.DOCS_WRITTEN, stats.docsWritten);
            oldApiCounter(reporter, Counters.BULK_WRITES, stats.bulkWrites);
            oldApiCounter(reporter, Counters.DOCS_RETRIED, stats.docsRetried);
            oldApiCounter(reporter, Counters.BULK_RETRIES, stats.bulkRetries);
            oldApiCounter(reporter, Counters.DOCS_READ, stats.docsRead);
            oldApiCounter(reporter, Counters.NODE_RETRIES, stats.nodeRetries);
            oldApiCounter(reporter, Counters.NET_RETRIES, stats.netRetries);
        }

        if (progressable instanceof TaskInputOutputContext) {
            TaskInputOutputContext tioc = (TaskInputOutputContext) progressable;

            newApiCounter(tioc, Counters.BYTES_WRITTEN, stats.bytesWritten);
            newApiCounter(tioc, Counters.BYTES_READ, stats.bytesRead);
            newApiCounter(tioc, Counters.DOCS_WRITTEN, stats.docsWritten);
            newApiCounter(tioc, Counters.BULK_WRITES, stats.bulkWrites);
            newApiCounter(tioc, Counters.DOCS_RETRIED, stats.docsRetried);
            newApiCounter(tioc, Counters.BULK_RETRIES, stats.bulkRetries);
            newApiCounter(tioc, Counters.DOCS_READ, stats.docsRead);
            newApiCounter(tioc, Counters.NODE_RETRIES, stats.nodeRetries);
            newApiCounter(tioc, Counters.NET_RETRIES, stats.netRetries);
        }
    }

    private static void oldApiCounter(Reporter reporter, Enum<?> counter, long value) {
        Counter c = reporter.getCounter(counter);
        if (c != null) {
            c.increment(value);
        }
    }

    @SuppressWarnings("unchecked")
    private static void newApiCounter(TaskInputOutputContext tioc, Enum<?> counter, long value) {
        org.apache.hadoop.mapreduce.Counter c = tioc.getCounter(counter);
        if (c != null) {
            c.increment(value);
        }
    }
}
