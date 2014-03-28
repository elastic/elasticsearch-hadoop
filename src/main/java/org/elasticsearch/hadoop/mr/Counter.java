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

import java.util.EnumSet;
import java.util.Set;

import org.elasticsearch.hadoop.rest.stats.Stats;

/**
 * Enum used for representing the stats inside Hadoop.
 */
public enum Counter {

    BYTES_WRITTEN {
        @Override
        public long get(Stats stats) {
            return stats.bytesWritten;
        }
    },
    DOCS_WRITTEN {
        @Override
        public long get(Stats stats) {
            return stats.docsWritten;
        }
    },
    BULK_WRITES {
        @Override
        public long get(Stats stats) {
            return stats.bulkWrites;
        }
    },
    BYTES_ACCEPTED {
        @Override
        public long get(Stats stats) {
            return stats.bytesAccepted;
        }
    },
    DOCS_ACCEPTED {
        @Override
        public long get(Stats stats) {
            return stats.docsAccepted;
        }
    },

    DOCS_RETRIED {
        @Override
        public long get(Stats stats) {
            return stats.docsRetried;
        }
    },
    BYTES_RETRIED {
        @Override
        public long get(Stats stats) {
            return stats.bytesRetried;
        }
    },
    BULK_RETRIES {
        @Override
        public long get(Stats stats) {
            return stats.bulkRetries;
        }
    },
    BYTES_READ {
        @Override
        public long get(Stats stats) {
            return stats.bytesRead;
        }
    },
    DOCS_READ {
        @Override
        public long get(Stats stats) {
            return stats.docsRead;
        }
    },
    NODE_RETRIES {
        @Override
        public long get(Stats stats) {
            return stats.nodeRetries;
        }
    },
    NET_RETRIES {
        @Override
        public long get(Stats stats) {
            return stats.netRetries;
        }
    },
    NET_TOTAL_TIME_MS {
        @Override
        public long get(Stats stats) {
            return stats.netTotalTime;
        }
    },
    BULK_TOTAL_TIME_MS {
        @Override
        public long get(Stats stats) {
            return stats.bulkTotalTime;
        }
    },
    BULK_RETRIES_TOTAL_TIME_MS {
        @Override
        public long get(Stats stats) {
            return stats.bulkRetriesTotalTime;
        }
    },
    SCROLL_READS {
        @Override
        public long get(Stats stats) {
            return stats.scrollReads;
        }
    },
    SCROLL_TOTAL_TIME_MS {
        @Override
        public long get(Stats stats) {
            return stats.scrollTotalTime;
        }
    };

    public static final Set<Counter> ALL = EnumSet.allOf(Counter.class);

    public abstract long get(Stats stats);
}