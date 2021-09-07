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

import java.io.InputStream;
import java.util.List;
import java.util.Map;

public interface Response {

    int status();

    String statusDescription();

    InputStream body();

    CharSequence uri();

    boolean isInformal();

    boolean isSuccess();

    boolean isRedirection();

    boolean isClientError();

    boolean isServerError();

    boolean hasSucceeded();

    boolean hasFailed();

    /**
     * Returns the headers with the given name
     * @param headerName, case-insensitive
     * @return The list of matching headers
     */
    List<String> getHeaders(String headerName);
}