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

package org.elasticsearch.hadoop.rest;

import java.util.BitSet;
import java.util.Collections;
import java.util.List;

/**
 * Simple response object that tracks useful information about a bulk indexing response.
 * This includes the determined response code, the number of docs indexed, the docs that
 * did not make it, and a sample of error messages for the user to peruse.
 */
public class BulkResponse {

    static BulkResponse ok(int totalWrites) {
        return new BulkResponse(totalWrites);
    }

    private final int httpStatus;
    private final int totalWrites;
    private final BitSet leftovers;
    private final List<String> errorExamples;

    /**
     * Creates a bulk response denoting that everything is OK
     * @param totalWrites
     */
    private BulkResponse(int totalWrites) {
        this(HttpStatus.OK, totalWrites, new BitSet(), Collections.<String>emptyList());
    }

    public BulkResponse(int httpStatus, int totalWrites, BitSet leftovers, List<String> errorExamples) {
        this.httpStatus = httpStatus;
        this.totalWrites = totalWrites;
        this.leftovers = leftovers;
        this.errorExamples = errorExamples;
    }

    public int getHttpStatus() {
        return httpStatus;
    }

    public int getTotalWrites() {
        return totalWrites;
    }

    public BitSet getLeftovers() {
        return leftovers;
    }

    public List<String> getErrorExamples() {
        return errorExamples;
    }
}
