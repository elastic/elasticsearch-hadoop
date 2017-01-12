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
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.hadoop.EsHadoopIllegalStateException;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.cfg.FieldPresenceValidation;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.rest.SearchRequestBuilder;
import org.elasticsearch.hadoop.rest.query.QueryUtils;
import org.elasticsearch.hadoop.rest.Resource;
import org.elasticsearch.hadoop.rest.RestRepository;
import org.elasticsearch.hadoop.rest.ScrollQuery;
import org.elasticsearch.hadoop.serialization.ScrollReader;
import org.elasticsearch.hadoop.serialization.ScrollReader.ScrollReaderConfig;
import org.elasticsearch.hadoop.serialization.builder.JdkValueReader;
import org.elasticsearch.hadoop.serialization.dto.mapping.Field;
import org.elasticsearch.hadoop.serialization.dto.mapping.MappingUtils;
import org.elasticsearch.hadoop.util.EsMajorVersion;
import org.elasticsearch.hadoop.util.StringUtils;

import cascading.flow.FlowProcess;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import cascading.tuple.TupleEntrySchemeCollector;
import cascading.tuple.TupleEntrySchemeIterator;

/**
 * Local Cascading Tap.
 */
class EsLocalTap extends Tap<Properties, ScrollQuery, Object> {

    private static final long serialVersionUID = 8644631529427137615L;

    private static Log log = LogFactory.getLog(EsLocalTap.class);
    private final String target;
    private final Properties tapProperties;

    public EsLocalTap(String host, int port, String resource, String query, Fields fields, Properties props) {
        super(new EsLocalScheme(host, port, resource, query, fields, props), SinkMode.UPDATE);
        this.target = resource;
        this.tapProperties = props;
    }

    @Override
    public String getIdentifier() {
        return target;
    }

    @Override
    public TupleEntryIterator openForRead(FlowProcess<Properties> flowProcess, ScrollQuery input) throws IOException {
        if (input == null) {
            // get original copy
            Settings settings = CascadingUtils.addDefaultsToSettings(CascadingUtils.extractOriginalProperties(flowProcess.getConfigCopy()), tapProperties, log);

            // will be closed by the query is finished
            RestRepository client = new RestRepository(settings);
            Field mapping = client.getMapping();
            Collection<String> fields = CascadingUtils.fieldToAlias(settings, getSourceFields());

            String userFilter = settings.getReadSourceFilter();
            if (StringUtils.hasText(userFilter)){
                if (fields.isEmpty()) {
                    fields = StringUtils.tokenize(userFilter, ",");
                } else {
                    throw new EsHadoopIllegalStateException("User specified source filters were found [" + userFilter + "], " +
                            "but the connector is executing in a state where it has provided its own source filtering " +
                            "[" + StringUtils.concatenate(fields, ",") + "]. Please clear the user specified source fields under the " +
                            "[" + ConfigurationOptions.ES_READ_SOURCE_FILTER + "] property to continue.");
                }
            }

            // validate if possible
            FieldPresenceValidation validation = settings.getReadFieldExistanceValidation();
            if (validation.isRequired()) {
                MappingUtils.validateMapping(fields, mapping, validation, log);
            }
            
            EsMajorVersion esVersion = settings.getInternalVersionOrThrow();
            Resource read = new Resource(settings, true);
            SearchRequestBuilder queryBuilder =
                    new SearchRequestBuilder(esVersion, settings.getReadMetadata() && settings.getReadMetadataVersion())
                            .types(read.type())
                            .indices(read.index())
                            .query(QueryUtils.parseQuery(settings))
                            .scroll(settings.getScrollKeepAlive())
                            .size(settings.getScrollSize())
                            .limit(settings.getScrollLimit())
                            .filters(QueryUtils.parseFilters(settings))
                            .fields(StringUtils.concatenate(fields, ","));
            input = queryBuilder.build(client, new ScrollReader(new ScrollReaderConfig(new JdkValueReader(), mapping, settings)));
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