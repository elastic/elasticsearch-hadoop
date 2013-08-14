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
package org.elasticsearch.hadoop.cfg;

import java.util.Enumeration;
import java.util.Properties;

import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.StringUtils;
import org.elasticsearch.hadoop.util.unit.Booleans;
import org.elasticsearch.hadoop.util.unit.ByteSizeValue;
import org.elasticsearch.hadoop.util.unit.TimeValue;

/**
 * Holder class containing the various configuration bits used by ElasticSearch Hadoop. Handles internally the fall back to defaults when looking for undefined, optional settings.
 */
public abstract class Settings implements InternalConfigurationOptions {

    private String host;
    private int port;
    private String targetResource;

    public String getHost() {
        return StringUtils.hasText(host) ? host : getProperty(ES_HOST, ES_HOST_DEFAULT);
    }

    public int getPort() {
        return (port > 0) ? port : Integer.valueOf(getProperty(ES_PORT, ES_PORT_DEFAULT));
    }

    public long getHttpTimeout() {
        return TimeValue.parseTimeValue(getProperty(ES_HTTP_TIMEOUT, ES_HTTP_TIMEOUT_DEFAULT)).getMillis();
    }

    public int getBatchSizeInBytes() {
        return ByteSizeValue.parseBytesSizeValue(getProperty(ES_BATCH_SIZE_BYTES, ES_BATCH_SIZE_BYTES_DEFAULT)).bytesAsInt();
    }

    public int getBatchSizeInEntries() {
        return Integer.valueOf(getProperty(ES_BATCH_SIZE_ENTRIES, ES_BATCH_SIZE_ENTRIES_DEFAULT));
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

    public String getSerializerValueWriterClassName() {
        return getProperty(ES_SERIALIZATION_WRITER_CLASS, null);
    }

    public String getSerializerValueReaderClassName() {
        return getProperty(ES_SERIALIZATION_READER_CLASS, null);
    }

    public boolean getIndexAutoCreate() {
        return Booleans.parseBoolean(getProperty(ES_INDEX_AUTO_CREATE, ES_INDEX_AUTO_CREATE_DEFAULT));
    }

    public String getTargetUri() {
        String address = getProperty(INTERNAL_ES_TARGET_URI);
        return (StringUtils.hasText(address) ? address: new StringBuilder("http://").append(getHost()).append(":").append(getPort()).append("/").toString());
    }

    public String getWriteStrategy() {
        return getProperty(ES_INDEX_WRITE_STRATEGY, ES_INDEX_WRITE_STRATEGY_DEFAULT);
    }

    public String getIdPath() {
        return getProperty(ES_ID_PATH, ES_ID_PATH_DEFAULT);
    }


    public Settings setHost(String host) {
        this.host = host;
        return this;
    }

    public Settings setPort(int port) {
        this.port = port;
        return this;
    }

    public Settings setResource(String index) {
        this.targetResource = index;
        return this;
    }

    public String getTargetResource() {
        String resource = getProperty(INTERNAL_ES_TARGET_RESOURCE);
        return (StringUtils.hasText(targetResource) ? targetResource : StringUtils.hasText(resource) ? resource : getProperty(ES_RESOURCE));
    }

    public Settings cleanUri() {
        setProperty(INTERNAL_ES_TARGET_URI, "");
        return this;
    }

    public Settings clean() {
        setProperty(INTERNAL_ES_TARGET_RESOURCE, "");
        cleanUri();
        return this;
    }

    /**
     * Saves the settings state after validating them.
     */
    public void save() {
        String targetUri = getTargetUri();
        String resource = getTargetResource();

        Assert.hasText(targetUri, "No address specified");
        Assert.hasText(resource, String.format("No resource (index/query/location) ['%s'] specified", ES_RESOURCE));

        setProperty(INTERNAL_ES_TARGET_URI, targetUri);
        setProperty(INTERNAL_ES_TARGET_RESOURCE, resource);
    }

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
        if (properties == null) {
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
}