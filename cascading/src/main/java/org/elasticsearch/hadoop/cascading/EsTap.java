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
package org.elasticsearch.hadoop.cascading;

import java.io.IOException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.hadoop.util.Version;
import org.slf4j.LoggerFactory;

import cascading.flow.Flow;
import cascading.flow.FlowElement;
import cascading.flow.FlowProcess;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

/**
 * Cascading Tap backed by ElasticSearch. Can be used as a source and/or sink, for both local and Hadoop (local or not) flows.
 * If no fields are specified or are associated with the incoming tuple, the Tap will create name each field "field[num]" - this allows the document to be parsed by ES but will most likely conflict with the
 * existing mapping for the given index.
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class EsTap extends Tap<Object, Object, Object> {

    private static final long serialVersionUID = 2062780701366901965L;

    private static Log log = LogFactory.getLog(EsTap.class);

    private static boolean logVersion = false;

    private String resource;
    private String query;
    private boolean runningInHadoop = false;
    private Tap actualTap;
    private Fields fields;
    private String host;
    private int port;
    private Properties props;

    // TODO: add defaults fall back
    public EsTap(String resource) {
        this(resource, null, null);
    }

    public EsTap(String resource, String query) {
        this(resource, query, null);
    }

    public EsTap(String host, int port, String resource) {
        this(host, port, resource, null, null);
    }

    public EsTap(String host, int port, String resource, Fields fields) {
        this(host, port, resource, null, fields);
    }

    public EsTap(String host, int port, String resource, String query) {
        this(host, port, resource, query, null);
    }

    public EsTap(String resource, Fields fields) {
        this(null, -1, resource, null, fields);
    }

    public EsTap(String resource, String query, Fields fields) {
        this(null, -1, resource, query, fields);
    }

    public EsTap(String host, int port, String resource, String query, Fields fields) {
        this(host, port, resource, query, fields, null);
    }

    public EsTap(String host, int port, String resource, String query, Fields fields, Properties tapSettings) {
        super(null, SinkMode.UPDATE);
        this.resource = resource;
        this.query = query;
        this.host = host;
        this.port = port;
        this.fields = fields;
        this.props = tapSettings;
    }

    @Override
    public void flowConfInit(Flow<Object> flow) {
        initInnerTapIfNotSet(flow, "cascading.flow.hadoop.HadoopFlow");
        actualTap.flowConfInit(flow);
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
        initInnerTapIfNotSetFromFlowProcess(flowProcess);
        actualTap.sourceConfInit(flowProcess, conf);
    }

    @Override
    public void sinkConfInit(FlowProcess<Object> flowProcess, Object conf) {
        initInnerTapIfNotSetFromFlowProcess(flowProcess);
        actualTap.sinkConfInit(flowProcess, conf);
    }

    @Override
    public TupleEntryIterator openForRead(FlowProcess<Object> flowProcess, Object input) throws IOException {
        initInnerTapIfNotSetFromFlowProcess(flowProcess);
        return actualTap.openForRead(flowProcess, input);
    }

    @Override
    public Fields retrieveSourceFields(FlowProcess<Object> flowProcess) {
        initInnerTapIfNotSetFromFlowProcess(flowProcess);
        return actualTap.retrieveSourceFields(flowProcess);
    }

    @Override
    public TupleEntryCollector openForWrite(FlowProcess<Object> flowProcess, Object output) throws IOException {
        initInnerTapIfNotSetFromFlowProcess(flowProcess);
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
        return (actualTap != null ? actualTap.toString() : getClass().getSimpleName() + "[\"" + getScheme() + "\"]" + "[not initialized]");
    }

    private void initInnerTapIfNotSetFromFlowProcess(FlowProcess<Object> target) {
        initInnerTapIfNotSet(target, "cascading.flow.hadoop.HadoopFlowProcess");
    }

    private void initInnerTapIfNotSet(Object target, String hadoopTypeName) {
        if (actualTap != null) {
            return;
        }

        Class<?> clz = null;
        try {
            clz = Class.forName(hadoopTypeName, false, getClass().getClassLoader());
            if (clz.isInstance(target)) {
                runningInHadoop = true;
            }
        } catch (ClassNotFoundException e) {
            runningInHadoop = false;
        }
        actualTap = (runningInHadoop ? new EsHadoopTap(host, port, resource, query, fields, props) : new EsLocalTap(host, port, resource, query, fields, props));
        setScheme(actualTap.getScheme());
        if (log.isDebugEnabled()) {
            log.debug(String.format("Detected %s environment; initializing [%s]", (runningInHadoop ? "Hadoop" : "local"), actualTap.getClass().getSimpleName()));
        }

        // use SLF4J just like Cascading
        if (!logVersion) {
            logVersion = true;
            LoggerFactory.getLogger(EsTap.class).info(String.format("Elasticsearch Hadoop %s initialized", Version.version()));
        }
    }
}