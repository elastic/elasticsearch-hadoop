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

import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.cfg.SettingsManager;
import org.elasticsearch.hadoop.rest.BufferedRestClient;
import org.elasticsearch.hadoop.rest.QueryBuilder;
import org.elasticsearch.hadoop.rest.ScrollQuery;
import org.elasticsearch.hadoop.serialization.JdkValueReader;
import org.elasticsearch.hadoop.serialization.ScrollReader;

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
class ESLocalTap extends Tap<Properties, ScrollQuery, Object> {

    private static final long serialVersionUID = 8644631529427137615L;

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
    public TupleEntryIterator openForRead(FlowProcess<Properties> flowProcess, ScrollQuery input) throws IOException {
        Settings settings = SettingsManager.loadFrom(flowProcess.getConfigCopy());
        // will be closed by the tuple once its done
        client = new BufferedRestClient(settings);

        if (input == null) {
            input = QueryBuilder.query(settings).build(client, new ScrollReader(new JdkValueReader(), null));
        }
        return new TupleEntrySchemeIterator<Properties, ScrollQuery>(flowProcess, getScheme(), input, getIdentifier());
    }

    @Override
    public TupleEntryCollector openForWrite(FlowProcess<Properties> flowProcess, Object output) throws IOException {
        return new TupleEntrySchemeCollector<Properties, Object>(flowProcess, getScheme(), output);
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