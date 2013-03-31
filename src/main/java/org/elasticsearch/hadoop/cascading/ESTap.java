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
 * Cascading Tap backed by ElasticSearch.
 * If no fields are specified or are associated with the incoming tuple, the Tap will create name each field "field[num]" - this allows the document to be parsed by ES but will most likely conflict with the
 * existing mapping for the given index.
 */
public class ESTap<Config, Output> extends Tap<Config, QueryResult, Output> {

    private String target;
    private BufferedRestClient client;

    // TODO: add defaults fallback
    public ESTap(String index) {
        this(index, null);
    }

    public ESTap(String host, int port, String index) {
        this(host, port, index, null);
    }

    public ESTap(String index, Fields fields) {
        this(null, -1, index, fields);
    }

    public ESTap(String host, int port, String index, Fields fields) {
        this.target = index;
        setScheme(new ESScheme(host, port, index, fields));
    }

    @Override
    public String getIdentifier() {
        return target;
    }

    @Override
    public void sourceConfInit(FlowProcess<Config> flowProcess, Config conf) {
        super.sourceConfInit(flowProcess, conf);
    }

    @Override
    public void sinkConfInit(FlowProcess<Config> flowProcess, Config conf) {
        super.sinkConfInit(flowProcess, conf);
    }

    @Override
    public TupleEntryIterator openForRead(FlowProcess<Config> flowProcess, QueryResult input) throws IOException {
        client = new BufferedRestClient(target);

        if (input == null) {
            input = client.query(target);
        }
        return new TupleEntrySchemeIterator(flowProcess, getScheme(), input, getIdentifier());
    }

    @Override
    public TupleEntryCollector openForWrite(FlowProcess<Config> flowProcess, Output output) throws IOException {
        return new TupleEntrySchemeCollector(flowProcess, getScheme(), output);
    }

    @Override
    public boolean createResource(Config conf) throws IOException {
        return false;
    }

    @Override
    public boolean deleteResource(Config conf) throws IOException {
        return false;
    }

    @Override
    public boolean resourceExists(Config conf) throws IOException {
        return true;
    }

    @Override
    public long getModifiedTime(Config conf) throws IOException {
        return -1;
    }
}