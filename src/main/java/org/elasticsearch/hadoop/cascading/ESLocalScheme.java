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
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.cfg.SettingsManager;
import org.elasticsearch.hadoop.rest.BufferedRestClient;
import org.elasticsearch.hadoop.rest.QueryResult;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.Tuples;

/**
 * Cascading Scheme handling
 */
class ESLocalScheme extends Scheme<Properties, QueryResult, Object, Object[], Object[]> {

    private final String resource;
    private final String host;
    private final int port;
    private transient BufferedRestClient client;

    ESLocalScheme(String host, int port, String index, Fields fields) {
        this.resource = index;
        this.host = host;
        this.port = port;
        if (fields != null) {
            setSinkFields(fields);
            setSourceFields(fields);
        }
    }

    @Override
    public void sourcePrepare(FlowProcess<Properties> flowProcess, SourceCall<Object[], QueryResult> sourceCall) throws IOException {
        super.sourcePrepare(flowProcess, sourceCall);

        Fields sourceCallFields = sourceCall.getIncomingEntry().getFields();
        Fields sourceFields = (sourceCallFields.isDefined() ? sourceCallFields : getSourceFields());
        List<String> tupleNames = resolveNames(sourceFields);

        Object[] context = new Object[1];
        context[0] = tupleNames;
        sourceCall.setContext(context);
    }

    @Override
    public void sourceCleanup(FlowProcess<Properties> flowProcess, SourceCall<Object[], QueryResult> sourceCall) throws IOException {
        sourceCall.getInput().close();
        cleanupClient();
    }

    @Override
    public void sinkCleanup(FlowProcess<Properties> flowProcess, SinkCall<Object[], Object> sinkCall) throws IOException {
        cleanupClient();
    }

    private void cleanupClient() throws IOException {
        if (client != null) {
            client.close();
            client = null;
        }
    }

    @Override
    public void sinkPrepare(FlowProcess<Properties> flowProcess, SinkCall<Object[], Object> sinkCall) throws IOException {
        super.sinkPrepare(flowProcess, sinkCall);

        Fields sinkCallFields = sinkCall.getOutgoingEntry().getFields();
        Fields sinkFields = (sinkCallFields.isDefined() ? sinkCallFields : getSinkFields());
        List<String> tupleNames = resolveNames(sinkFields);

        Object[] context = new Object[1];
        context[0] = tupleNames;
        sinkCall.setContext(context);
    }

    private List<String> resolveNames(Fields fields) {

        //TODO: add handling of undefined types (Fields.UNKNOWN/ALL/RESULTS...)
        if (fields == null || !fields.isDefined()) {
            // use auto-generated name
            return Collections.emptyList();
        }

        int size = fields.size();
        List<String> names = new ArrayList<String>(size);
        for (int fieldIndex = 0; fieldIndex < size; fieldIndex++) {
            names.add(fields.get(fieldIndex).toString());
        }

        return names;
    }

    @Override
    public void sourceConfInit(FlowProcess<Properties> flowProcess, Tap<Properties, QueryResult, Object> tap, Properties conf) {
        initClient(conf);
    }

    @Override
    public void sinkConfInit(FlowProcess<Properties> flowProcess, Tap<Properties, QueryResult, Object> tap, Properties conf) {
        initClient(conf);
    }

    private void initClient(Properties props) {
        if (client == null) {
            Settings settings = SettingsManager.loadFrom(props).setHost(host).setPort(port).setResource(resource);
            settings.save();
            client = new BufferedRestClient(settings);
        }
    }

    @Override
    public boolean source(FlowProcess<Properties> flowProcess, SourceCall<Object[], QueryResult> sourceCall) throws IOException {
        QueryResult query = sourceCall.getInput();
        if (query.hasNext()) {
            Map<String, Object> map = query.next();
            TupleEntry tuples = sourceCall.getIncomingEntry();
            // TODO: verify ordering guarantees
            Set<String> keys = map.keySet();
            //tuples.set(new TupleEntry(new Fields(keys.toArray(new String[keys.size()])),

            tuples.setTuple(Tuples.create(new ArrayList<Object>(map.values())));
            return true;
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void sink(FlowProcess<Properties> flowProcess, SinkCall<Object[], Object> sinkCall) throws IOException {
        Tuple tuple = sinkCall.getOutgoingEntry().getTuple();

        List<String> names = (List<String>) sinkCall.getContext()[0];
        Map<String, Object> toES = new LinkedHashMap<String, Object>();
        for (int i = 0; i < tuple.size(); i++) {
            String name = (i < names.size() ? names.get(i) : "tuple" + i);
            toES.put(name, tuple.getObject(i));
        }

        client.addToIndex(toES);
    }
}