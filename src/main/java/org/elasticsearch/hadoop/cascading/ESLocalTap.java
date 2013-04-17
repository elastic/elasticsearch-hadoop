/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticsearch.hadoop.cascading;

import java.io.IOException;
import java.util.Properties;

import org.elasticsearch.hadoop.cfg.SettingsManager;
import org.elasticsearch.hadoop.rest.BufferedRestClient;
import org.elasticsearch.hadoop.rest.QueryResult;

import cascading.flow.FlowProcess;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import cascading.tuple.TupleEntrySchemeCollector;
import cascading.tuple.TupleEntrySchemeIterator;

/**
 * Local Cascading Tap.
 */
// - local-mode  Tap<Properties, QueryResult, ?>
class ESLocalTap extends Tap<Properties, QueryResult, Object> {

    private String target;
    private BufferedRestClient client;

    public ESLocalTap(String host, int port, String resource, Fields fields) {
        this.target = resource;
        setScheme(new ESLocalScheme(host, port, resource, fields));
    }

    @Override
    public String getIdentifier() {
        return target;
    }

    @Override
    public TupleEntryIterator openForRead(FlowProcess<Properties> flowProcess, QueryResult input) throws IOException {
        client = new BufferedRestClient(SettingsManager.loadFrom(flowProcess.getConfigCopy()));

        if (input == null) {
            input = client.query(target);
        }
        return new TupleEntrySchemeIterator(flowProcess, getScheme(), input, getIdentifier());
    }

    @Override
    public TupleEntryCollector openForWrite(FlowProcess<Properties> flowProcess, Object output) throws IOException {
        return new TupleEntrySchemeCollector(flowProcess, getScheme(), output);
    }

    @Override
    public boolean createResource(Properties conf) throws IOException {
        return false;
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
        return -1;
    }
}