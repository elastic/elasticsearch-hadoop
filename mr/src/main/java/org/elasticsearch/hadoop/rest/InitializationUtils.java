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
package org.elasticsearch.hadoop.rest;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.cfg.HadoopSettingsManager;
import org.elasticsearch.hadoop.cfg.InternalConfigurationOptions;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.serialization.BytesConverter;
import org.elasticsearch.hadoop.serialization.builder.ContentBuilder;
import org.elasticsearch.hadoop.serialization.builder.NoOpValueWriter;
import org.elasticsearch.hadoop.serialization.builder.ValueReader;
import org.elasticsearch.hadoop.serialization.builder.ValueWriter;
import org.elasticsearch.hadoop.serialization.field.FieldExtractor;
import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.BytesArray;
import org.elasticsearch.hadoop.util.FastByteArrayOutputStream;
import org.elasticsearch.hadoop.util.SettingsUtils;
import org.elasticsearch.hadoop.util.StringUtils;

public abstract class InitializationUtils {

    public static void checkIdForOperation(Settings settings) {
        String operation = settings.getOperation();

        if (ConfigurationOptions.ES_OPERATION_UPDATE.equals(operation)) {
            Assert.isTrue(StringUtils.hasText(settings.getMappingId()),
                    String.format("Operation [%s] requires an id but none (%s) was specified", operation, ConfigurationOptions.ES_MAPPING_ID));
        }
    }

    public static boolean discoverNodesIfNeeded(Settings settings, Log log) {
        if (settings.getNodesDiscovery()) {
            RestClient bootstrap = new RestClient(settings);

            try {
                List<String> discoveredNodes = bootstrap.discoverNodes();
                if (log.isDebugEnabled()) {
                    log.debug(String.format("Nodes discovery enabled - found %s", discoveredNodes));
                }

                SettingsUtils.addDiscoveredNodes(settings, discoveredNodes);
            } finally {
                bootstrap.close();
            }
            return true;
        }

        return false;
    }

    public static void filterNonClientNodesIfNeeded(Settings settings, Log log) {
        if (!settings.getNodesClientOnly()) {
            return;
        }

        RestClient bootstrap = new RestClient(settings);
        try {
            List<String> clientNodes = bootstrap.getHttpClientNodes();
            if (clientNodes.isEmpty()) {
                throw new EsHadoopIllegalArgumentException("Client-only routing specified but no client nodes with HTTP-enabled were found in the cluster...");
            }
            if (log.isDebugEnabled()) {
                log.debug(String.format("Found client nodes %s", clientNodes));
            }

            List<String> ddNodes = SettingsUtils.discoveredOrDeclaredNodes(settings);
            // remove non-client nodes
            ddNodes.retainAll(clientNodes);
            if (log.isDebugEnabled()) {
                log.debug(String.format("Filtered discovered only nodes %s to client-only %s", SettingsUtils.discoveredOrDeclaredNodes(settings), ddNodes));
            }

            if (ddNodes.isEmpty()) {
                String message = "Client-only routing specified but no client nodes with HTTP-enabled available; ";
                if (settings.getNodesDiscovery()) {
                    message += String.format("looks like the client nodes discovered have been removed; is the cluster in a stable state? %s", clientNodes);
                }
                else {
                    message += String.format("node discovery is disabled and none of nodes specified fits the criterion %s", SettingsUtils.discoveredOrDeclaredNodes(settings));
                }
                throw new EsHadoopIllegalArgumentException(message);
            }

            SettingsUtils.setDiscoveredNodes(settings, ddNodes);
        } finally {
            bootstrap.close();
        }
    }

    public static void filterNonDataNodesIfNeeded(Settings settings, Log log) {
        if (!settings.getNodesDataOnly()) {
          return;
        }

        RestClient bootstrap = new RestClient(settings);
        try  {
            List<String> dataNodes = bootstrap.getHttpDataNodes();
            if (dataNodes.isEmpty()) {
                throw new EsHadoopIllegalArgumentException("Data node only routing specified but no data nodes with HTTP-enabled were found in the cluster...");
            }
            if (log.isDebugEnabled()) {
                log.debug(String.format("Found data nodes %s", dataNodes));
            }

            List<String> ddNodes = SettingsUtils.discoveredOrDeclaredNodes(settings);
            // remove non-data nodes
            ddNodes.retainAll(dataNodes);
            if (log.isDebugEnabled()) {
                log.debug(String.format("Filtered discovered only nodes %s to data-only %s", SettingsUtils.discoveredOrDeclaredNodes(settings), ddNodes));
            }

            if (ddNodes.isEmpty()) {
                String message = "Data node only routing specified but no data nodes with HTTP-enabled available; ";
                if (settings.getNodesDiscovery()) {
                    message += String.format("looks like the data nodes discovered have been removed; is the cluster in a stable state? %s", dataNodes);
                }
                else {
                    message += String.format("node discovery is disabled and none of nodes specified fits the criterion %s", SettingsUtils.discoveredOrDeclaredNodes(settings));
                }
                throw new EsHadoopIllegalArgumentException(message);
            }

            SettingsUtils.setDiscoveredNodes(settings, dataNodes);
        } finally {
            bootstrap.close();
        }
    }

    public static String discoverEsVersion(Settings settings, Log log) {
        String version = settings.getProperty(InternalConfigurationOptions.INTERNAL_ES_VERSION);
        if (StringUtils.hasText(version)) {
            if (log.isDebugEnabled()) {
                log.debug(String.format("Elasticsearch version [%s] already present in configuration; skipping discovery", version));
            }

            return version;
        }

        RestClient bootstrap = new RestClient(settings);
        // first get ES version
        try {
            String esVersion = bootstrap.esVersion();
            if (log.isDebugEnabled()) {
                log.debug(String.format("Discovered Elasticsearch version [%s]", esVersion));
            }
            settings.setProperty(InternalConfigurationOptions.INTERNAL_ES_VERSION, esVersion);
            return esVersion;
        } finally {
            bootstrap.close();
        }
    }

    public static void checkIndexExistence(RestRepository client) {
        checkIndexExistence(client.getSettings(), client);
    }

    public static void checkIndexExistence(Settings settings, RestRepository client) {
        // check index existence
        if (!settings.getIndexAutoCreate()) {
            if (client == null) {
                client = new RestRepository(settings);
            }
            if (!client.indexExists(false)) {
                client.close();
                throw new EsHadoopIllegalArgumentException(String.format("Target index [%s] does not exist and auto-creation is disabled [setting '%s' is '%s']",
                        settings.getResourceWrite(), ConfigurationOptions.ES_INDEX_AUTO_CREATE, settings.getIndexAutoCreate()));
            }
        }
    }

    public static boolean setFieldExtractorIfNotSet(Settings settings, Class<? extends FieldExtractor> clazz, Log log) {
        if (!StringUtils.hasText(settings.getMappingIdExtractorClassName())) {
            Log logger = (log != null ? log : LogFactory.getLog(clazz));

            String name = clazz.getName();
            settings.setProperty(ConfigurationOptions.ES_MAPPING_DEFAULT_EXTRACTOR_CLASS, name);
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("Using pre-defined field extractor [%s] as default", settings.getMappingIdExtractorClassName()));
            }
            return true;
        }

        return false;
    }

    public static <T> void saveSchemaIfNeeded(Object conf, ValueWriter<T> schemaWriter, T schema, Log log) {
        Settings settings = HadoopSettingsManager.loadFrom(conf);

        if (settings.getIndexAutoCreate()) {
            RestRepository client = new RestRepository(settings);
            if (!client.indexExists(false)) {
                if (schemaWriter == null) {
                    log.warn(String.format("No mapping found [%s] and no schema found; letting Elasticsearch perform auto-mapping...",  settings.getResourceWrite()));
                }
                else {
                    log.info(String.format("No mapping found [%s], creating one based on given schema", settings.getResourceWrite()));
                    ContentBuilder builder = ContentBuilder.generate(schemaWriter).value(schema).flush();
                    BytesArray content = ((FastByteArrayOutputStream) builder.content()).bytes();
                    builder.close();
                    client.putMapping(content);
                    if (log.isDebugEnabled()) {
                        log.debug(String.format("Creating ES mapping [%s] from schema [%s]", content.toString(), schema));
                    }
                }
            }
            client.close();
        }
    }

    public static boolean setValueWriterIfNotSet(Settings settings, Class<? extends ValueWriter<?>> clazz, Log log) {
        if (!StringUtils.hasText(settings.getSerializerValueWriterClassName())) {
            Log logger = (log != null ? log : LogFactory.getLog(clazz));

            String name = clazz.getName();
            if (settings.getInputAsJson()) {
                name = NoOpValueWriter.class.getName();
                if (logger.isDebugEnabled()) {
                    logger.debug(String.format("Elasticsearch input marked as JSON; bypassing serialization through [%s] instead of [%s]", name, clazz));
                }
            }
            settings.setProperty(ConfigurationOptions.ES_SERIALIZATION_WRITER_VALUE_CLASS, name);
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("Using pre-defined writer serializer [%s] as default", settings.getSerializerValueWriterClassName()));
            }
            return true;
        }

        return false;
    }

    public static boolean setBytesConverterIfNeeded(Settings settings, Class<? extends BytesConverter> clazz, Log log) {
        if (settings.getInputAsJson() && !StringUtils.hasText(settings.getSerializerBytesConverterClassName())) {
            settings.setProperty(ConfigurationOptions.ES_SERIALIZATION_WRITER_BYTES_CLASS, clazz.getName());
            Log logger = (log != null ? log : LogFactory.getLog(clazz));
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("JSON input specified; using pre-defined bytes/json converter [%s] as default", settings.getSerializerBytesConverterClassName()));
            }
            return true;
        }

        return false;
    }

    public static boolean setValueReaderIfNotSet(Settings settings, Class<? extends ValueReader> clazz, Log log) {

        if (!StringUtils.hasText(settings.getSerializerValueReaderClassName())) {
            settings.setProperty(ConfigurationOptions.ES_SERIALIZATION_READER_VALUE_CLASS, clazz.getName());
            Log logger = (log != null ? log : LogFactory.getLog(clazz));
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("Using pre-defined reader serializer [%s] as default", settings.getSerializerValueReaderClassName()));
            }
            return true;
        }

        return false;
    }
}