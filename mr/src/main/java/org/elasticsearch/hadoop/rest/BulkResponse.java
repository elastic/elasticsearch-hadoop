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

import java.util.Collections;
import java.util.List;

import org.elasticsearch.hadoop.util.BytesArray;

/**
 * Simple response object that tracks useful information about a bulk indexing response.
 * This includes the determined response code, the number of docs indexed, the docs that
 * did not make it, and a sample of error messages for the user to peruse.
 */
public class BulkResponse {

    static BulkResponse complete(int httpStatus, int totalWrites) {
        return new BulkResponse(BulkStatus.COMPLETE, httpStatus, null, totalWrites, Collections.<BulkError>emptyList());
    }

    static BulkResponse partial(int httpStatus, int totalWrites, List<BulkError> errors) {
        return new BulkResponse(BulkStatus.PARTIAL, httpStatus, null, totalWrites, errors);
    }

    static BulkResponse failed(int httpStatus, String errorMessage, int totalDroppedWrites) {
        return new BulkResponse(BulkStatus.FAILED, httpStatus, errorMessage, totalDroppedWrites, Collections.<BulkError>emptyList());
    }

    public enum BulkStatus {
        /**
         * The bulk operation was completed successfully with all documents accepted
         */
        COMPLETE,
        /**
         * The bulk operation was completed partially, with some documents containing failures
         */
        PARTIAL,
        /**
         * The bulk operation itself failed, potentially due to non-document related errors
         */
        FAILED
    }

    public static class BulkError {

        private final int position;
        private final BytesArray document;
        private final int documentStatus;
        private final String errorMessage;

        public BulkError(int position, BytesArray document, int documentStatus, String errorMessage) {
            this.position = position;
            this.document = document;
            this.documentStatus = documentStatus;
            this.errorMessage = errorMessage;
        }

        public int getPosition() {
            return position;
        }

        public BytesArray getDocument() {
            return document;
        }

        public int getDocumentStatus() {
            return documentStatus;
        }

        public String getErrorMessage() {
            return errorMessage;
        }
    }

    private final BulkStatus status;
    private final int httpStatus;
    private final int totalDocs;
    private final List<BulkError> documentErrors;
    private final String errorMessage;

    private BulkResponse(BulkStatus status, int httpStatus, String errorMessage, int totalDocs, List<BulkError> documentErrors) {
        this.status = status;
        this.httpStatus = httpStatus;
        this.errorMessage = errorMessage;
        this.totalDocs = totalDocs;
        this.documentErrors = documentErrors;
    }

    public BulkStatus getStatus() {
        return status;
    }

    public int getHttpStatus() {
        return httpStatus;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public int getTotalDocs() {
        return totalDocs;
    }

    public List<BulkError> getDocumentErrors() {
        return documentErrors;
    }
}
