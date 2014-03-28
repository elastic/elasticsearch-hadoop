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
import org.apache.hadoop.util.Progressable;
import org.elasticsearch.hadoop.mr.compat.CompatHandler;
import org.elasticsearch.hadoop.mr.compat.TaskInputOutputContext;
import org.elasticsearch.hadoop.rest.stats.Stats;

class ReportingUtils {

    // handles Hadoop 'old' and 'new' API reporting classes, namely {@link Reporter} and {@link TaskInputOutputContext}
    @SuppressWarnings({ "rawtypes" })
    static void report(Progressable progressable, Stats stats) {
        progressable = (Progressable) CompatHandler.unwrap(progressable);

        if (progressable == null || progressable == Reporter.NULL) {
            return;
        }

        if (progressable instanceof Reporter) {
            Reporter reporter = (Reporter) progressable;
            for (Counter count : Counter.ALL) {
                oldApiCounter(reporter, count, count.get(stats));
            }
        }

        if (progressable instanceof org.apache.hadoop.mapreduce.TaskInputOutputContext) {
            TaskInputOutputContext compatTioc = CompatHandler.taskInputOutputContext((org.apache.hadoop.mapreduce.TaskInputOutputContext) progressable);
            for (Counter count : Counter.ALL) {
                newApiCounter(compatTioc, count, count.get(stats));
            }
        }
    }

    private static void oldApiCounter(Reporter reporter, Enum<?> counter, long value) {
        org.apache.hadoop.mapred.Counters.Counter c = reporter.getCounter(counter);
        if (c != null) {
            c.increment(value);
        }
    }

    private static void newApiCounter(TaskInputOutputContext tioc, Enum<?> counter, long value) {
        org.apache.hadoop.mapreduce.Counter c = tioc.getCounter(counter);
        if (c != null) {
            CompatHandler.counter(c).increment(value);
        }
    }
}