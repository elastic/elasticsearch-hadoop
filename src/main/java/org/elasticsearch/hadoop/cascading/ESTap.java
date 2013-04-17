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

import cascading.flow.Flow;
import cascading.flow.FlowElement;
import cascading.flow.FlowProcess;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

/**
 * Cascading Tap backed by ElasticSearch. Can be used as a source and/or sink, for both local and Hadoop (local or not) flows.
 * If no fields are specified or are associated with the incoming tuple, the Tap will create name each field "field[num]" - this allows the document to be parsed by ES but will most likely conflict with the
 * existing mapping for the given index.
 */
public class ESTap extends Tap<Object, Object, Object> {

    private String resource;
    private boolean runningInHadoop = false;
    private Tap actualTap;
    private Fields fields;
    private String host;
    private int port;

    // TODO: add defaults fallback
    public ESTap(String resource) {
        this(resource, null);
    }

    public ESTap(String host, int port, String resource) {
        this(host, port, resource, null);
    }

    public ESTap(String resource, Fields fields) {
        this(null, -1, resource, fields);
    }

    public ESTap(String host, int port, String resource, Fields fields) {
        this.resource = resource;
        this.host = host;
        this.port = port;
        this.fields = fields;
    }

    @Override
    public void flowConfInit(Flow<Object> flow) {
        Class<?> clz = null;
        try {
            clz = Class.forName("cascading.flow.hadoop.HadoopFlow", false, getClass().getClassLoader());
            if (clz.isInstance(flow)) {
                runningInHadoop = true;
            }
        } catch (ClassNotFoundException e) {
            runningInHadoop = false;
        }
        // TODO: alternative
        //hadoopFlow = flow.getConfig() instanceof Configuration;

        actualTap = (runningInHadoop ? new ESHadoopTap(host, port, resource, fields) : new ESLocalTap(host, port, resource, fields));
        setScheme(actualTap.getScheme());
    }


    @Override
    public boolean isSink() {
        return true;
    }

    @Override
    public boolean isSource() {
        return true;
    }

    @Override
    public String getIdentifier() {
        return resource;
    }

    @Override
    public void sourceConfInit(FlowProcess<Object> flowProcess, Object conf) {
        actualTap.sourceConfInit(flowProcess, conf);
    }

    @Override
    public void sinkConfInit(FlowProcess<Object> flowProcess, Object conf) {
        actualTap.sinkConfInit(flowProcess, conf);
    }

    @Override
    public TupleEntryIterator openForRead(FlowProcess<Object> flowProcess, Object input) throws IOException {
        return actualTap.openForRead(flowProcess, input);
    }

    @Override
    public TupleEntryCollector openForWrite(FlowProcess<Object> flowProcess, Object output) throws IOException {
        return actualTap.openForWrite(flowProcess, output);
    }

    @Override
    public boolean createResource(Object conf) throws IOException {
        return actualTap.createResource(conf);
    }

    @Override
    public boolean deleteResource(Object conf) throws IOException {
        return actualTap.deleteResource(conf);
    }

    @Override
    public boolean resourceExists(Object conf) throws IOException {
        return actualTap.resourceExists(conf);
    }

    @Override
    public long getModifiedTime(Object conf) throws IOException {
        return actualTap.getModifiedTime(conf);
    }

    @Override
    public boolean isEquivalentTo(FlowElement element) {
        return actualTap.isEquivalentTo(element);
    }

    @Override
    public String toString() {
        return actualTap.toString();
    }
}