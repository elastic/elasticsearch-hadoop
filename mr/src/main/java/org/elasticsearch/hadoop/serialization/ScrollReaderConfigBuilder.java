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

package org.elasticsearch.hadoop.serialization;

import java.util.List;

import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.handler.HandlerLoader;
import org.elasticsearch.hadoop.handler.impl.PreloadedHandlerLoader;
import org.elasticsearch.hadoop.serialization.builder.ValueReader;
import org.elasticsearch.hadoop.serialization.dto.mapping.Mapping;
import org.elasticsearch.hadoop.serialization.handler.read.IDeserializationErrorHandler;
import org.elasticsearch.hadoop.serialization.handler.read.impl.DeserializationHandlerLoader;
import org.elasticsearch.hadoop.util.StringUtils;

public class ScrollReaderConfigBuilder {

    public static ScrollReaderConfigBuilder builder(ValueReader reader, Mapping resolvedMapping, Settings settings) {
        return builder(reader, settings).setResolvedMapping(resolvedMapping);
    }

    public static ScrollReaderConfigBuilder builder(ValueReader reader, Settings settings) {
        return new ScrollReaderConfigBuilder(settings, reader);
    }

    // Integration specific object factory
    private final ValueReader reader;

    // Record Typing
    private boolean returnRawJson;

    // Mappings
    private Mapping resolvedMapping;
    private boolean ignoreUnmappedFields;

    // Metadata Fields
    private boolean readMetadata;
    private String metadataName;

    // Field Filtering
    private List<String> includeFields;
    private List<String> excludeFields;
    private List<String> includeArrayFields;

    private HandlerLoader<IDeserializationErrorHandler> errorHandlerLoader;

    public ScrollReaderConfigBuilder(Settings settings, ValueReader reader) {
        this.reader = reader;

        // Source defaults from Settings
        this.returnRawJson = settings.getOutputAsJson();
        this.ignoreUnmappedFields = settings.getReadMappingMissingFieldsIgnore();
        this.readMetadata = settings.getReadMetadata();
        this.metadataName = settings.getReadMetadataField();
        this.includeFields = StringUtils.tokenize(settings.getReadFieldInclude());
        this.excludeFields = StringUtils.tokenize(settings.getReadFieldExclude());
        this.includeArrayFields = StringUtils.tokenize(settings.getReadFieldAsArrayInclude());

        DeserializationHandlerLoader loader = new DeserializationHandlerLoader();
        loader.setSettings(settings);
        this.errorHandlerLoader = loader;

        // No default value
        this.resolvedMapping = null;
    }

    public ValueReader getReader() {
        return reader;
    }

    public boolean getReturnRawJson() {
        return returnRawJson;
    }

    public ScrollReaderConfigBuilder setReturnRawJson(boolean returnRawJson) {
        this.returnRawJson = returnRawJson;
        return this;
    }

    public Mapping getResolvedMapping() {
        return resolvedMapping;
    }

    public ScrollReaderConfigBuilder setResolvedMapping(Mapping resolvedMapping) {
        this.resolvedMapping = resolvedMapping;
        return this;
    }

    public boolean getIgnoreUnmappedFields() {
        return ignoreUnmappedFields;
    }

    public ScrollReaderConfigBuilder setIgnoreUnmappedFields(boolean ignoreUnmappedFields) {
        this.ignoreUnmappedFields = ignoreUnmappedFields;
        return this;
    }

    public boolean getReadMetadata() {
        return readMetadata;
    }

    public ScrollReaderConfigBuilder setReadMetadata(boolean readMetadata) {
        this.readMetadata = readMetadata;
        return this;
    }

    public String getMetadataName() {
        return metadataName;
    }

    public ScrollReaderConfigBuilder setMetadataName(String metadataName) {
        this.metadataName = metadataName;
        return this;
    }

    public List<String> getIncludeFields() {
        return includeFields;
    }

    public ScrollReaderConfigBuilder setIncludeFields(List<String> includeFields) {
        this.includeFields = includeFields;
        return this;
    }

    public List<String> getExcludeFields() {
        return excludeFields;
    }

    public ScrollReaderConfigBuilder setExcludeFields(List<String> excludeFields) {
        this.excludeFields = excludeFields;
        return this;
    }

    public List<String> getIncludeArrayFields() {
        return includeArrayFields;
    }

    public ScrollReaderConfigBuilder setIncludeArrayFields(List<String> includeArrayFields) {
        this.includeArrayFields = includeArrayFields;
        return this;
    }

    public HandlerLoader<IDeserializationErrorHandler> getErrorHandlerLoader() {
        return errorHandlerLoader;
    }

    public ScrollReaderConfigBuilder setErrorHandlerLoader(HandlerLoader<IDeserializationErrorHandler> errorHandlerLoader) {
        this.errorHandlerLoader = errorHandlerLoader;
        return this;
    }

    public ScrollReaderConfigBuilder setErrorHandlers(List<IDeserializationErrorHandler> errorHandlers) {
        this.errorHandlerLoader = new PreloadedHandlerLoader<IDeserializationErrorHandler>(errorHandlers);
        return this;
    }
}
