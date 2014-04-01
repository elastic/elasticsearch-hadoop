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
package org.elasticsearch.hadoop.integration.cascading;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Properties;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tap.SinkTap;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntrySchemeCollector;

public class OutputStreamTap extends SinkTap<Properties, OutputStream> {
    private final OutputStream os;

    public OutputStreamTap(Scheme<Properties, ?, OutputStream, ?, ?> scheme, OutputStream os) {
        super(scheme, SinkMode.UPDATE);
        this.os = os;
    }

    @Override
    public String getIdentifier() {
        return "outputstream";
    }

    @Override
    public TupleEntryCollector openForWrite(FlowProcess<Properties> flowProcess, OutputStream output)
            throws IOException {
        return new TupleEntrySchemeCollector<Properties, OutputStream>(flowProcess, getScheme(), os);
    }

    @Override
    public boolean createResource(Properties conf) throws IOException {
        return true;
    }

    @Override
    public boolean deleteResource(Properties conf) throws IOException {
        return false;
    }

    @Override
    public boolean resourceExists(Properties conf) throws IOException {
        return true;
    }

    @Override
    public long getModifiedTime(Properties conf) throws IOException {
        return 0;
    }
}
