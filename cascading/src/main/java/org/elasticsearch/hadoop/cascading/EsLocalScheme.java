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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.LogFactory;
import org.elasticsearch.hadoop.cfg.HadoopSettingsManager;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.mr.Counter;
import org.elasticsearch.hadoop.rest.InitializationUtils;
import org.elasticsearch.hadoop.rest.RestRepository;
import org.elasticsearch.hadoop.rest.ScrollQuery;
import org.elasticsearch.hadoop.rest.stats.Stats;
import org.elasticsearch.hadoop.util.FieldAlias;
import org.elasticsearch.hadoop.util.StringUtils;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import static org.elasticsearch.hadoop.cascading.CascadingValueWriter.SINK_CTX_ALIASES;
import static org.elasticsearch.hadoop.cascading.CascadingValueWriter.SINK_CTX_SIZE;

/**
 * Cascading Scheme handling
 */
class EsLocalScheme extends Scheme<Properties, ScrollQuery, Object, Object[], Object[]> {

    private static final long serialVersionUID = 979036202776892844L;

    private final static int SRC_CTX_SIZE = 2;
    private final static int SRC_CTX_ALIASES = 0;
    private final static int SRC_CTX_OUTPUT_JSON = 1;

    private final String resource;
    private final String query;
    private final String host;
    private final int port;
    private final Properties props;
    private transient RestRepository client;

    EsLocalScheme(String host, int port, String index, String query, Fields fields, Properties props) {
        this.resource = index;
        this.query = query;
        this.host = host;
        this.port = port;
        if (fields != null) {
            setSinkFields(fields);
            setSourceFields(fields);
        }
        this.props = props;
    }

    @Override
    public void sourcePrepare(FlowProcess<Properties> flowProcess, SourceCall<Object[], ScrollQuery> sourceCall) throws IOException {
        super.sourcePrepare(flowProcess, sourceCall);

        Object[] context = new Object[SRC_CTX_SIZE];
        Settings settings = HadoopSettingsManager.loadFrom(flowProcess.getConfigCopy()).merge(props);
        context[SRC_CTX_ALIASES] = CascadingUtils.alias(settings);
        context[SRC_CTX_OUTPUT_JSON] = settings.getOutputAsJson();
        sourceCall.setContext(context);
    }

    @Override
    public void sourceCleanup(FlowProcess<Properties> flowProcess, SourceCall<Object[], ScrollQuery> sourceCall) throws IOException {
        // in case of a source there's no local client so do all reporting here
        report(sourceCall.getInput().stats(), flowProcess);
        report(sourceCall.getInput().repository().stats(), flowProcess);
        sourceCall.getInput().close();
        sourceCall.setContext(null);
        // used for consistency
        cleanupClient(flowProcess);
    }

    @Override
    public void sinkCleanup(FlowProcess<Properties> flowProcess, SinkCall<Object[], Object> sinkCall) throws IOException {
        cleanupClient(flowProcess);
    }

    private void cleanupClient(FlowProcess<Properties> flowProcess) throws IOException {
        if (client != null) {
            client.close();
            report(client.stats(), flowProcess);
            client = null;
        }
    }

    private void report(Stats stats, FlowProcess<Properties> flowProcess) {
        // report current stats
        for (Counter count : Counter.ALL) {
            flowProcess.increment(count, count.get(stats));
        }
    }

    @Override
    public void sinkPrepare(FlowProcess<Properties> flowProcess, SinkCall<Object[], Object> sinkCall) throws IOException {
        super.sinkPrepare(flowProcess, sinkCall);

        Object[] context = new Object[SINK_CTX_SIZE];
        Settings settings = HadoopSettingsManager.loadFrom(flowProcess.getConfigCopy()).merge(props);
        context[SINK_CTX_ALIASES] = CascadingUtils.fieldToAlias(settings, getSinkFields());
        sinkCall.setContext(context);
    }

    @Override
    public void sourceConfInit(FlowProcess<Properties> flowProcess, Tap<Properties, ScrollQuery, Object> tap, Properties conf) {
        initClient(conf, true);
    }

    @Override
    public void sinkConfInit(FlowProcess<Properties> flowProcess, Tap<Properties, ScrollQuery, Object> tap, Properties conf) {
        initClient(conf, false);
        InitializationUtils.checkIndexExistence(client);
    }

    private void initClient(Properties props, boolean read) {
        if (client == null) {
            Settings settings = CascadingUtils.addDefaultsToSettings(props, this.props, LogFactory.getLog(EsTap.class));
            CascadingUtils.init(settings, host, port, resource, query, read);
            CascadingUtils.initialDiscovery(settings, LogFactory.getLog(EsTap.class));
            CascadingUtils.finalValidation(settings, read);
            client = new RestRepository(settings);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean source(FlowProcess<Properties> flowProcess, SourceCall<Object[], ScrollQuery> sourceCall) throws IOException {
        ScrollQuery query = sourceCall.getInput();

        if (!query.hasNext()) {
            return false;
        }

        boolean isJSON = (Boolean) sourceCall.getContext()[SRC_CTX_OUTPUT_JSON];

        TupleEntry entry = sourceCall.getIncomingEntry();

        Map<String, Object> data;
        if (isJSON) {
            data = new HashMap<String, Object>(1);
            data.put("data", query.next()[1]);
        } else {
            data = (Map<String, Object>) query.next()[1];
        }

        FieldAlias alias = (FieldAlias) sourceCall.getContext()[SRC_CTX_ALIASES];

        if (entry.getFields().isDefined()) {
            // lookup using writables
            for (Comparable<?> field : entry.getFields()) {
                Object result = data;
                // check for multi-level alias
                for (String level : StringUtils.tokenize(alias.toES(field.toString()), ".")) {
                    result = ((Map) result).get(level);
                    if (result == null) {
                        break;
                    }
                }
                entry.setObject(field, result);
            }
        }
        else {
            // no definition means no coercion
            List<Object> elements = Tuple.elements(entry.getTuple());
            elements.clear();
            elements.addAll(data.values());
        }
        return true;
    }

    @Override
    public void sink(FlowProcess<Properties> flowProcess, SinkCall<Object[], Object> sinkCall) throws IOException {
        client.writeToIndex(sinkCall);
    }
}