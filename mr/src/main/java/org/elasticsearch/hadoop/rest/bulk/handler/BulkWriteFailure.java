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

package org.elasticsearch.hadoop.rest.bulk.handler;

import java.io.InputStream;
import java.util.Collections;
import java.util.List;

import org.elasticsearch.hadoop.handler.impl.BaseExceptional;
import org.elasticsearch.hadoop.util.BytesArray;
import org.elasticsearch.hadoop.util.FastByteArrayInputStream;

/**
 * Encapsulates all available information pertaining to an unhandled bulk indexing operation failure.
 */
public class BulkWriteFailure extends BaseExceptional {

    private final int response;
    private final BytesArray contents;
    private final int attemptNumber;

    public BulkWriteFailure(int response, Exception reason, BytesArray contents, int attemptNumber, List<String> passReasons) {
        super(reason, passReasons);
        this.response = response;
        this.contents = contents;
        this.attemptNumber = attemptNumber;
    }

    /**
     * @return HTTP Response code for entry
     */
    public int getResponseCode() {
        return response;
    }

    /**
     * @return serialized bulk entry in byte array format
     */
    public InputStream getEntryContents() {
        return new FastByteArrayInputStream(contents);
    }

    /**
     * The number of times that this version of the document has been sent to Elasticsearch. In the event that a document
     * is revised by a handler before being retried, this number will reset in the event of another failure. If a document
     * is retried without being revised, this number will increment across retries.
     *
     * @return the number of bulk write attempts.
     */
    public int getNumberOfAttempts() {
        return attemptNumber;
    }
}
