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
package org.elasticsearch.hadoop.cfg;

import java.io.InputStream;
import java.util.Enumeration;
import java.util.Locale;
import java.util.Properties;

import org.apache.commons.logging.LogFactory;
import org.elasticsearch.hadoop.util.IOUtils;
import org.elasticsearch.hadoop.util.StringUtils;
import org.elasticsearch.hadoop.util.unit.Booleans;
import org.elasticsearch.hadoop.util.unit.ByteSizeValue;
import org.elasticsearch.hadoop.util.unit.TimeValue;

/**
 * Holder class containing the various configuration bits used by ElasticSearch Hadoop. Handles internally the fall back to defaults when looking for undefined, optional settings.
 */
public abstract class Settings implements InternalConfigurationOptions {

    private static boolean ES_HOST_WARNING = true;

    public String getNodes() {
        String host = getProperty(ES_HOST);
        if (StringUtils.hasText(host)) {
            if (ES_HOST_WARNING) {
                LogFactory.getLog(Settings.class).warn(String.format("`%s` property has been deprecated - use `%s` instead", ES_HOST, ES_NODES));
                ES_HOST_WARNING = false;
            }
            return host;
        }

        return getProperty(ES_NODES, ES_NODES_DEFAULT);
    }

    public int getPort() {
        return Integer.valueOf(getProperty(ES_PORT, ES_PORT_DEFAULT));
    }

    public boolean getNodesDiscovery() {
        return Booleans.parseBoolean(getProperty(ES_NODES_DISCOVERY, ES_NODES_DISCOVERY_DEFAULT));
    }

    public long getHttpTimeout() {
        return TimeValue.parseTimeValue(getProperty(ES_HTTP_TIMEOUT, ES_HTTP_TIMEOUT_DEFAULT)).getMillis();
    }

    public int getHttpRetries() {
        return Integer.valueOf(getProperty(ES_HTTP_RETRIES, ES_HTTP_RETRIES_DEFAULT));
    }

    public int getBatchSizeInBytes() {
        return ByteSizeValue.parseBytesSizeValue(getProperty(ES_BATCH_SIZE_BYTES, ES_BATCH_SIZE_BYTES_DEFAULT)).bytesAsInt();
    }

    public int getBatchSizeInEntries() {
        return Integer.valueOf(getProperty(ES_BATCH_SIZE_ENTRIES, ES_BATCH_SIZE_ENTRIES_DEFAULT));
    }

    public int getBatchWriteRetryCount() {
        return Integer.parseInt(getProperty(ES_BATCH_WRITE_RETRY_COUNT, ES_BATCH_WRITE_RETRY_COUNT_DEFAULT));
    }

    public long getBatchWriteRetryWait() {
        return TimeValue.parseTimeValue(getProperty(ES_BATCH_WRITE_RETRY_WAIT, ES_BATCH_WRITE_RETRY_WAIT_DEFAULT)).getMillis();
    }

    public String getBatchWriteRetryPolicy() {
        return getProperty(ES_BATCH_WRITE_RETRY_POLICY, ES_BATCH_WRITE_RETRY_POLICY_DEFAULT);
    }

    public boolean getBatchRefreshAfterWrite() {
        return Booleans.parseBoolean(getProperty(ES_BATCH_WRITE_REFRESH, ES_BATCH_WRITE_REFRESH_DEFAULT));
    }

    public long getScrollKeepAlive() {
        return TimeValue.parseTimeValue(getProperty(ES_SCROLL_KEEPALIVE, ES_SCROLL_KEEPALIVE_DEFAULT)).getMillis();
    }

    public long getScrollSize() {
        return Long.valueOf(getProperty(ES_SCROLL_SIZE, ES_SCROLL_SIZE_DEFAULT));
    }

    public String getScrollFields() {
        String internalFields = getProperty(INTERNAL_ES_TARGET_FIELDS);
        return (StringUtils.hasText(internalFields) ? internalFields : getProperty(ES_SCROLL_FIELDS));
    }

    public String getSerializerValueWriterClassName() {
        return getProperty(ES_SERIALIZATION_WRITER_VALUE_CLASS);
    }

    public String getSerializerBytesConverterClassName() {
        return getProperty(ES_SERIALIZATION_WRITER_BYTES_CLASS);
    }

    public String getSerializerValueReaderClassName() {
        return getProperty(ES_SERIALIZATION_READER_VALUE_CLASS);
    }

    public boolean getIndexAutoCreate() {
        return Booleans.parseBoolean(getProperty(ES_INDEX_AUTO_CREATE, ES_INDEX_AUTO_CREATE_DEFAULT));
    }

    public boolean getIndexReadMissingAsEmpty() {
        return Booleans.parseBoolean(getProperty(ES_INDEX_READ_MISSING_AS_EMPTY, ES_INDEX_READ_MISSING_AS_EMPTY_DEFAULT));
    }

    public boolean getInputAsJson() {
        return Booleans.parseBoolean(getProperty(ES_INPUT_JSON, ES_INPUT_JSON_DEFAULT));
    }

    public String getOperation() {
        return getProperty(ES_WRITE_OPERATION, ES_WRITE_OPERATION_DEFAULT).toLowerCase(Locale.ENGLISH);
    }

    public String getMappingId() {
        return getProperty(ES_MAPPING_ID);
    }

    public String getMappingParent() {
        return getProperty(ES_MAPPING_PARENT);
    }

    public String getMappingVersion() {
        return getProperty(ES_MAPPING_VERSION);
    }

    public String getMappingRouting() {
        return getProperty(ES_MAPPING_ROUTING);
    }

    public String getMappingTtl() {
        return getProperty(ES_MAPPING_TTL);
    }

    public String getMappingTimestamp() {
        return getProperty(ES_MAPPING_TIMESTAMP);
    }

    public String getMappingDefaultClassExtractor() {
        return getProperty(ES_MAPPING_DEFAULT_EXTRACTOR_CLASS);
    }

    public String getMappingIdExtractorClassName() {
        return getProperty(ES_MAPPING_ID_EXTRACTOR_CLASS, getMappingDefaultClassExtractor());
    }

    public String getMappingParentExtractorClassName() {
        return getProperty(ES_MAPPING_PARENT_EXTRACTOR_CLASS, getMappingDefaultClassExtractor());
    }

    public String getMappingVersionExtractorClassName() {
        return getProperty(ES_MAPPING_VERSION_EXTRACTOR_CLASS, getMappingDefaultClassExtractor());
    }

    public String getMappingRoutingExtractorClassName() {
        return getProperty(ES_MAPPING_ROUTING_EXTRACTOR_CLASS, getMappingDefaultClassExtractor());
    }

    public String getMappingTtlExtractorClassName() {
        return getProperty(ES_MAPPING_TTL_EXTRACTOR_CLASS, getMappingDefaultClassExtractor());
    }

    public String getMappingTimestampExtractorClassName() {
        return getProperty(ES_MAPPING_TIMESTAMP_EXTRACTOR_CLASS, getMappingDefaultClassExtractor());
    }

    public String getMappingIndexFormatClassName() {
        return getProperty(ES_MAPPING_INDEX_FORMAT_CLASS, ES_MAPPING_DEFAULT_INDEX_FORMAT_CLASS);
    }

    public boolean getUpsertDoc() {
        return Booleans.parseBoolean(getProperty(ES_UPSERT_DOC, ES_UPSERT_DOC_DEFAULT));
    }

    public boolean getFieldReadEmptyAsNull() {
        return Booleans.parseBoolean(getProperty(ES_FIELD_READ_EMPTY_AS_NULL, ES_FIELD_READ_EMPTY_AS_NULL_DEFAULT));
    }

    public TimeValue getHeartBeatLead() {
        return TimeValue.parseTimeValue(getProperty(ES_HEART_BEAT_LEAD, ES_HEART_BEAT_LEAD_DEFAULT));
    }

    public String getNetworkProxyHttpHost() {
        return getProperty(ES_NET_PROXY_HTTP_HOST);
    }

    public int getNetworkProxyHttpPort() {
        return Integer.valueOf(getProperty(ES_NET_PROXY_HTTP_PORT, "-1"));
    }

    public String getNetworkProxyHttpUser() {
        return getProperty(ES_NET_PROXY_HTTP_USER);
    }

    public String getNetworkProxyHttpPass() {
        return getProperty(ES_NET_PROXY_HTTP_PASS);
    }

    public boolean getNetworkHttpUseSystemProperties() {
        return Booleans.parseBoolean(getProperty(ES_NET_PROXY_HTTP_USE_SYSTEM_PROPS, ES_NET_PROXY_HTTP_USE_SYSTEM_PROPS_DEFAULT));
    }

    public String getNetworkProxySocksHost() {
        return getProperty(ES_NET_PROXY_SOCKS_HOST);
    }

    public int getNetworkProxySocksPort() {
        return Integer.valueOf(getProperty(ES_NET_PROXY_SOCKS_PORT, "-1"));
    }

    public String getNetworkProxySocksUser() {
        return getProperty(ES_NET_PROXY_SOCKS_USER);
    }

    public String getNetworkProxySocksPass() {
        return getProperty(ES_NET_PROXY_SOCKS_PASS);
    }

    public boolean getNetworkSocksUseSystemProperties() {
        return Booleans.parseBoolean(getProperty(ES_NET_PROXY_SOCKS_USE_SYSTEM_PROPS, ES_NET_PROXY_SOCKS_USE_SYSTEM_PROPS_DEFAULT));
    }

    public Settings setHosts(String hosts) {
        setProperty(ES_NODES, hosts);
        return this;
    }

    public Settings setPort(int port) {
        setProperty(ES_PORT, "" + port);
        return this;
    }

    public Settings setResourceRead(String index) {
        setProperty(ES_RESOURCE_READ, index);
        return this;
    }

    public Settings setResourceWrite(String index) {
        setProperty(ES_RESOURCE_WRITE, index);
        return this;
    }

    public Settings setQuery(String query) {
        setProperty(ES_QUERY, StringUtils.hasText(query) ? query : "");
        return this;
    }

    protected String getResource() {
        return getProperty(ES_RESOURCE);
    }

    public String getResourceRead() {
        return getProperty(ES_RESOURCE_READ, getResource());
    }

    public String getResourceWrite() {
        return getProperty(ES_RESOURCE_WRITE, getResource());
    }

    String getTargetHosts() {
        String hosts = getProperty(INTERNAL_ES_HOSTS);
        return (StringUtils.hasText(hosts) ? hosts : getNodes());
    }

    public String getQuery() {
        return getProperty(ES_QUERY);
    }

    public abstract InputStream loadResource(String location);

    public abstract Settings copy();

    protected String getProperty(String name, String defaultValue) {
        String value = getProperty(name);
        if (!StringUtils.hasText(value)) {
            return defaultValue;
        }
        return value;
    }

    public abstract String getProperty(String name);

    public abstract void setProperty(String name, String value);

    public Settings merge(Properties properties) {
        if (properties == null || properties.isEmpty()) {
            return this;
        }

        Enumeration<?> propertyNames = properties.propertyNames();

        Object prop = null;
        for (; propertyNames.hasMoreElements();) {
            prop = propertyNames.nextElement();
            if (prop instanceof String) {
                Object value = properties.get(prop);
                setProperty((String) prop, value.toString());
            }
        }

        return this;
    }

    public Settings load(String source) {
        Properties copy = IOUtils.propsFromString(source);
        merge(copy);
        return this;
    }

    public String save() {
        Properties copy = asProperties();
        return IOUtils.propsToString(copy);
    }

    protected abstract Properties asProperties();
}