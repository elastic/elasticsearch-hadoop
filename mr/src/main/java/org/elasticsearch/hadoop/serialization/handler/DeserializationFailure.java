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

package org.elasticsearch.hadoop.serialization.handler;

import java.io.InputStream;
import java.util.Collections;
import java.util.List;

import org.elasticsearch.hadoop.handler.Exceptional;
import org.elasticsearch.hadoop.util.BytesArray;
import org.elasticsearch.hadoop.util.FastByteArrayInputStream;

public class DeserializationFailure implements Exceptional {
    private final BytesArray hit;
    private final Exception reason;
    private final List<String> passReasons;

    public DeserializationFailure(Exception reason, BytesArray hit, List<String> passReasons) {
        this.hit = hit;
        this.reason = reason;
        this.passReasons = Collections.unmodifiableList(passReasons);
    }

    /**
     * @return the scroll search hit in the original raw JSON format
     */
    public InputStream getHitContents() {
        return new FastByteArrayInputStream(hit);
    }

    @Override
    public Exception getException() {
        return reason;
    }

    @Override
    public List<String> previousHandlerMessages() {
        return passReasons;
    }
}
