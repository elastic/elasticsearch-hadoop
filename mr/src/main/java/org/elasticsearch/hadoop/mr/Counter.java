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

    BYTES_SENT {
        @Override
        public long get(Stats stats) {
            return stats.bytesSent;
        }
    },
    DOCS_SENT {
        @Override
        public long get(Stats stats) {
            return stats.docsSent;
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
    BYTES_RECEIVED {
        @Override
        public long get(Stats stats) {
            return stats.bytesReceived;
        }
    },
    DOCS_RECEIVED {
        @Override
        public long get(Stats stats) {
            return stats.docsReceived;
        }
    },
    BULK_TOTAL {
        @Override
        public long get(Stats stats) {
            return stats.bulkTotal;
        }
    },
    BULK_RETRIES {
        @Override
        public long get(Stats stats) {
            return stats.bulkRetries;
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
    SCROLL_TOTAL {
        @Override
        public long get(Stats stats) {
            return stats.scrollTotal;
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