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
import java.util.Collection;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.elasticsearch.hadoop.cfg.InternalConfigurationOptions;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.cfg.HadoopSettingsManager;
import org.elasticsearch.hadoop.mr.EsInputFormat;
import org.elasticsearch.hadoop.mr.EsOutputFormat;
import org.elasticsearch.hadoop.mr.HadoopCfgUtils;
import org.elasticsearch.hadoop.rest.InitializationUtils;
import org.elasticsearch.hadoop.rest.RestClient;
import org.elasticsearch.hadoop.serialization.builder.JdkValueReader;
import org.elasticsearch.hadoop.util.FieldAlias;
import org.elasticsearch.hadoop.util.SettingsUtils;
import org.elasticsearch.hadoop.util.StringUtils;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Cascading Scheme handling
 */
@SuppressWarnings("rawtypes")
class EsHadoopScheme extends Scheme<JobConf, RecordReader, OutputCollector, Object[], Object[]> {

    private static final long serialVersionUID = 4304172465362298925L;

    private final String index;
    private final String query;
    private final String nodes;
    private final int port;
    private final Properties props;
    private boolean IS_ES_10;

    private static Log log = LogFactory.getLog(EsHadoopScheme.class);

    EsHadoopScheme(String nodes, int port, String index, String query, Fields fields, Properties props) {
        this.index = index;
        this.query = query;
        this.nodes = nodes;
        this.port = port;
        if (fields != null) {
            setSinkFields(fields);
            setSourceFields(fields);
        }
        this.props = props;
    }

    @Override
    public void sourcePrepare(FlowProcess<JobConf> flowProcess, SourceCall<Object[], RecordReader> sourceCall) throws IOException {
        super.sourcePrepare(flowProcess, sourceCall);

        Object[] context = new Object[3];
        context[0] = sourceCall.getInput().createKey();
        context[1] = sourceCall.getInput().createValue();
        // as the tuple _might_ vary (some objects might be missing), we use a map rather then a collection
        Settings settings = loadSettings(flowProcess.getConfigCopy(), true);
        context[2] = CascadingUtils.alias(settings);
        sourceCall.setContext(context);
        IS_ES_10 = SettingsUtils.isEs10(settings);
    }

    @Override
    public void sourceCleanup(FlowProcess<JobConf> flowProcess, SourceCall<Object[], RecordReader> sourceCall) throws IOException {
        super.sourceCleanup(flowProcess, sourceCall);

        sourceCall.setContext(null);
    }

    @Override
    public void sinkPrepare(FlowProcess<JobConf> flowProcess, SinkCall<Object[], OutputCollector> sinkCall) throws IOException {
        super.sinkPrepare(flowProcess, sinkCall);

        Object[] context = new Object[1];
        // the tuple is fixed, so we can just use a collection/index
        Settings settings = loadSettings(flowProcess.getConfigCopy(), false);
        context[0] = CascadingUtils.fieldToAlias(settings, getSinkFields());
        sinkCall.setContext(context);
        IS_ES_10 = SettingsUtils.isEs10(settings);
    }

    public void sinkCleanup(FlowProcess<JobConf> flowProcess, SinkCall<Object[], OutputCollector> sinkCall) throws IOException {
        super.sinkCleanup(flowProcess, sinkCall);

        sinkCall.setContext(null);
    }

    @Override
    public Fields retrieveSourceFields(final FlowProcess<JobConf> flowProcess, final Tap tap) {
        Settings settings = loadSettings(flowProcess.getConfigCopy(), false);
        if (getSourceFields() == Fields.UNKNOWN && settings.getFieldDetection()) {
            log.info("resource: " + index);
            String[] parts = index.split("/");
            if (parts.length == 2) {
                String myIndex = parts[0];
                String docType = parts[1];
                log.info("index: " + myIndex + ", type: " + docType);

                String mappingsUrl = "/" + myIndex + "/_mapping/" + docType;
                log.info("mapping URL: " + mappingsUrl);
                RestClient client = new RestClient(settings);
                try {
                    String responseBody = IOUtils.toString(client.getRaw(mappingsUrl));

                    // extract fields from the response body
                    JsonNode mappingsObj = new ObjectMapper().readTree(responseBody)
                        .path(myIndex).path("mappings")
                        .path(docType).path("properties");
                    Iterator<String> fieldsIterator = mappingsObj.getFieldNames();
                    List<String> fieldList = new ArrayList<String>();
                    while (fieldsIterator.hasNext())
                        fieldList.add(fieldsIterator.next());
                    String[] fieldNames = new String[fieldList.size()];
                    fieldList.toArray(fieldNames);
                    Fields fields = new Fields(fieldNames);
                    log.info("fields: " + fieldNames);
                    setSourceFields(fields);
                } catch (IOException e) {
                    log.info("no fields found in the mapping");
                } finally {
                    client.close();
                }
            }
        }
        return getSourceFields();
    }

    @Override
    public void sourceConfInit(FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {
        conf.setInputFormat(EsInputFormat.class);
        Settings set = loadSettings(conf, true);

        Collection<String> fields = CascadingUtils.fieldToAlias(set, getSourceFields());
        // load only the necessary fields
        conf.set(InternalConfigurationOptions.INTERNAL_ES_TARGET_FIELDS, StringUtils.concatenateAndUriEncode(fields, ","));

        if (log.isTraceEnabled()) {
            log.trace("Initialized (source) configuration " + HadoopCfgUtils.asProperties(conf));
        }
    }

    @Override
    public void sinkConfInit(FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {

        conf.setOutputFormat(EsOutputFormat.class);
        // define an output dir to prevent Cascading from setting up a TempHfs and overriding the OutputFormat
        Settings set = loadSettings(conf, false);

        Log log = LogFactory.getLog(EsTap.class);
        InitializationUtils.setValueWriterIfNotSet(set, CascadingValueWriter.class, log);
        InitializationUtils.setValueReaderIfNotSet(set, JdkValueReader.class, log);
        InitializationUtils.setBytesConverterIfNeeded(set, CascadingLocalBytesConverter.class, log);
        InitializationUtils.setFieldExtractorIfNotSet(set, CascadingFieldExtractor.class, log);

        // NB: we need to set this property even though it is not being used - and since and URI causes problem, use only the resource/file
        //conf.set("mapred.output.dir", set.getTargetUri() + "/" + set.getTargetResource());
        HadoopCfgUtils.setFileOutputFormatDir(conf, set.getResourceWrite());
        HadoopCfgUtils.setOutputCommitterClass(conf, EsOutputFormat.EsOldAPIOutputCommitter.class.getName());

        if (log.isTraceEnabled()) {
            log.trace("Initialized (sink) configuration " + HadoopCfgUtils.asProperties(conf));
        }
    }

    private Settings loadSettings(Object source, boolean read) {
        return CascadingUtils.init(HadoopSettingsManager.loadFrom(source).merge(props), nodes, port, index, query, read);
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean source(FlowProcess<JobConf> flowProcess, SourceCall<Object[], RecordReader> sourceCall) throws IOException {
        Object[] context = sourceCall.getContext();

        if (!sourceCall.getInput().next(context[0], context[1])) {
            return false;
        }

        TupleEntry entry = sourceCall.getIncomingEntry();
        Map data = (Map) context[1];
        FieldAlias alias = (FieldAlias) context[2];

        if (entry.getFields().isDefined()) {
            // lookup using writables
            Text lookupKey = new Text();
            // TODO: it's worth benchmarking whether using an index/offset yields significantly better performance
            for (Comparable<?> field : entry.getFields()) {
                if (IS_ES_10) {
                    // check for multi-level alias
                    Object result = data;
                    for (String level : StringUtils.tokenize(alias.toES(field.toString()), ".")) {
                        lookupKey.set(level);
                        result = ((Map) result).get(lookupKey);
                        if (result == null) {
                            break;
                        }
                    }
                    CascadingUtils.setObject(entry, field, result);
                }
                else {
                    lookupKey.set(alias.toES(field.toString()));
                    CascadingUtils.setObject(entry, field, data.get(lookupKey));
                }
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

    @SuppressWarnings("unchecked")
    @Override
    public void sink(FlowProcess<JobConf> flowProcess, SinkCall<Object[], OutputCollector> sinkCall) throws IOException {
        sinkCall.getOutput().collect(null, sinkCall);
    }
}