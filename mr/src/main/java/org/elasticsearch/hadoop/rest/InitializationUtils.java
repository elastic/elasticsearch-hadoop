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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.hadoop.EsHadoopException;
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.EsHadoopIllegalStateException;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.cfg.InternalConfigurationOptions;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.security.UserProvider;
import org.elasticsearch.hadoop.serialization.BytesConverter;
import org.elasticsearch.hadoop.serialization.builder.NoOpValueWriter;
import org.elasticsearch.hadoop.serialization.builder.ValueReader;
import org.elasticsearch.hadoop.serialization.builder.ValueWriter;
import org.elasticsearch.hadoop.serialization.bulk.MetadataExtractor;
import org.elasticsearch.hadoop.serialization.dto.NodeInfo;
import org.elasticsearch.hadoop.serialization.field.FieldExtractor;
import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.ClusterInfo;
import org.elasticsearch.hadoop.util.ClusterName;
import org.elasticsearch.hadoop.util.EsMajorVersion;
import org.elasticsearch.hadoop.util.SettingsUtils;
import org.elasticsearch.hadoop.util.StringUtils;

public abstract class InitializationUtils {

    private static final Log LOG = LogFactory.getLog(InitializationUtils.class);

    public static void checkIdForOperation(Settings settings) {
        String operation = settings.getOperation();

        if (ConfigurationOptions.ES_OPERATION_UPDATE.equals(operation)) {
            Assert.isTrue(StringUtils.hasText(settings.getMappingId()),
                    String.format("Operation [%s] requires an id but none (%s) was specified", operation, ConfigurationOptions.ES_MAPPING_ID));
        }
    }

    public static void checkIndexNameForRead(Settings settings) {
        Resource readResource = new Resource(settings, true);
        if (readResource.index().contains("{") && readResource.index().contains("}")) {
            throw new EsHadoopIllegalArgumentException("Cannot read indices that have curly brace field extraction patterns in them: " + readResource.index());
        }
    }

    public static void checkIndexStatus(Settings settings) {
        if (!settings.getIndexReadAllowRedStatus()) {
            RestClient bootstrap = new RestClient(settings);
            Resource readResource = new Resource(settings, true);

            try {
                if (bootstrap.indexExists(readResource.index())) {
                    RestClient.Health status = bootstrap.getHealth(readResource.index());
                    if (status == RestClient.Health.RED) {
                        throw new EsHadoopIllegalStateException("Index specified [" + readResource.index() + "] is either red or " +
                                "includes an index that is red, and thus all requested data cannot be safely and fully loaded. " +
                                "Bailing out...");
                    }
                }
            } finally {
                bootstrap.close();
            }
        }
    }

    public static List<NodeInfo> discoverNodesIfNeeded(Settings settings, Log log) {
        if (settings.getNodesDiscovery()) {
            RestClient bootstrap = new RestClient(settings);

            try {
                List<NodeInfo> discoveredNodes = bootstrap.getHttpNodes(false);
                if (log.isDebugEnabled()) {
                    log.debug(String.format("Nodes discovery enabled - found %s", discoveredNodes));
                }

                SettingsUtils.addDiscoveredNodes(settings, discoveredNodes);
                return discoveredNodes;
            } finally {
                bootstrap.close();
            }
        }

        return null;
    }

    public static void filterNonClientNodesIfNeeded(Settings settings, Log log) {
        if (!settings.getNodesClientOnly()) {
            return;
        }

        RestClient bootstrap = new RestClient(settings);
        try {
            String message = "Client-only routing specified but no client nodes with HTTP-enabled available";
            List<NodeInfo> clientNodes = bootstrap.getHttpClientNodes();
            if (clientNodes.isEmpty()) {
                throw new EsHadoopIllegalArgumentException(message);
            }
            if (log.isDebugEnabled()) {
                log.debug(String.format("Found client nodes %s", clientNodes));
            }
            List<String> toRetain = new ArrayList<String>(clientNodes.size());
            for (NodeInfo node : clientNodes) {
                toRetain.add(node.getPublishAddress());
            }
            List<String> ddNodes = SettingsUtils.discoveredOrDeclaredNodes(settings);
            // remove non-client nodes
            ddNodes.retainAll(toRetain);
            if (log.isDebugEnabled()) {
                log.debug(String.format("Filtered discovered only nodes %s to client-only %s", SettingsUtils.discoveredOrDeclaredNodes(settings), ddNodes));
            }

            if (ddNodes.isEmpty()) {
                if (settings.getNodesDiscovery()) {
                    message += String.format("; looks like the client nodes discovered have been removed; is the cluster in a stable state? %s", clientNodes);
                }
                else {
                    message += String.format("; node discovery is disabled and none of nodes specified fit the criterion %s", SettingsUtils.discoveredOrDeclaredNodes(settings));
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
        try {
            String message = "No data nodes with HTTP-enabled available";
            List<NodeInfo> dataNodes = bootstrap.getHttpDataNodes();
            if (dataNodes.isEmpty()) {
                throw new EsHadoopIllegalArgumentException(message);
            }
            if (log.isDebugEnabled()) {
                log.debug(String.format("Found data nodes %s", dataNodes));
            }
            List<String> toRetain = new ArrayList<String>(dataNodes.size());
            for (NodeInfo node : dataNodes) {
                toRetain.add(node.getPublishAddress());
            }
            List<String> ddNodes = SettingsUtils.discoveredOrDeclaredNodes(settings);
            // remove non-data nodes
            ddNodes.retainAll(toRetain);
            if (log.isDebugEnabled()) {
                log.debug(String.format("Filtered discovered only nodes %s to data-only %s", SettingsUtils.discoveredOrDeclaredNodes(settings), ddNodes));
            }

            if (ddNodes.isEmpty()) {
                if (settings.getNodesDiscovery()) {
                    message += String.format("; looks like the data nodes discovered have been removed; is the cluster in a stable state? %s", dataNodes);
                }
                else {
                    message += String.format("; node discovery is disabled and none of nodes specified fit the criterion %s", SettingsUtils.discoveredOrDeclaredNodes(settings));
                }
                throw new EsHadoopIllegalArgumentException(message);
            }

            SettingsUtils.setDiscoveredNodes(settings, ddNodes);
        } finally {
            bootstrap.close();
        }
    }

    public static void filterNonIngestNodesIfNeeded(Settings settings, Log log) {
        if (!settings.getNodesIngestOnly()) {
            return;
        }

        RestClient bootstrap = new RestClient(settings);
        try {
            String message = "Ingest-only routing specified but no ingest nodes with HTTP-enabled available";
            List<NodeInfo> clientNodes = bootstrap.getHttpIngestNodes();
            if (clientNodes.isEmpty()) {
                throw new EsHadoopIllegalArgumentException(message);
            }
            if (log.isDebugEnabled()) {
                log.debug(String.format("Found ingest nodes %s", clientNodes));
            }
            List<String> toRetain = new ArrayList<String>(clientNodes.size());
            for (NodeInfo node : clientNodes) {
                toRetain.add(node.getPublishAddress());
            }
            List<String> ddNodes = SettingsUtils.discoveredOrDeclaredNodes(settings);
            // remove non-client nodes
            ddNodes.retainAll(toRetain);
            if (log.isDebugEnabled()) {
                log.debug(String.format("Filtered discovered only nodes %s to ingest-only %s", SettingsUtils.discoveredOrDeclaredNodes(settings), ddNodes));
            }

            if (ddNodes.isEmpty()) {
                if (settings.getNodesDiscovery()) {
                    message += String.format("; looks like the ingest nodes discovered have been removed; is the cluster in a stable state? %s", clientNodes);
                }
                else {
                    message += String.format("; node discovery is disabled and none of nodes specified fit the criterion %s", SettingsUtils.discoveredOrDeclaredNodes(settings));
                }
                throw new EsHadoopIllegalArgumentException(message);
            }

            SettingsUtils.setDiscoveredNodes(settings, ddNodes);
        } finally {
            bootstrap.close();
        }
    }

    public static void validateSettings(Settings settings) {
        // wan means all node restrictions are off the table
        if (settings.getNodesWANOnly()) {
            Assert.isTrue(!settings.getNodesDiscovery(), "Discovery cannot be enabled when running in WAN mode");
            Assert.isTrue(!settings.getNodesClientOnly(), "Client-only nodes cannot be enabled when running in WAN mode");
            Assert.isTrue(!settings.getNodesDataOnly(), "Data-only nodes cannot be enabled when running in WAN mode");
            Assert.isTrue(!settings.getNodesIngestOnly(), "Ingest-only nodes cannot be enabled when running in WAN mode");
        }

        // pick between data or client or ingest only nodes
        boolean alreadyRestricted = false;
        boolean[] restrictions = {settings.getNodesClientOnly(), settings.getNodesDataOnly(), settings.getNodesIngestOnly()};
        for (boolean restriction : restrictions) {
            Assert.isTrue((alreadyRestricted && restriction) == false, "Use either client-only or data-only or ingest-only nodes but not a combination");
            alreadyRestricted = alreadyRestricted || restriction;
        }

        // field inclusion/exclusion + input as json does not mix and the user should be informed.
        if (settings.getInputAsJson()) {
            Assert.isTrue(settings.getMappingIncludes().isEmpty(), "When writing data as JSON, the field inclusion feature is ignored. This is most likely not what the user intended. Bailing out...");
            Assert.isTrue(settings.getMappingExcludes().isEmpty(), "When writing data as JSON, the field exclusion feature is ignored. This is most likely not what the user intended. Bailing out...");
        }

        //check the configuration is coherent in order to use the delete operation
        if (ConfigurationOptions.ES_OPERATION_DELETE.equals(settings.getOperation())) {
            Assert.isTrue(!settings.getInputAsJson(), "When using delete operation, providing data as JSON is not coherent because this operation does not need document as a payload. This is most likely not what the user intended. Bailing out...");
            Assert.isTrue(settings.getMappingIncludes().isEmpty(), "When using delete operation, the field inclusion feature is ignored. This is most likely not what the user intended. Bailing out...");
            Assert.isTrue(settings.getMappingExcludes().isEmpty(), "When using delete operation, the field exclusion feature is ignored. This is most likely not what the user intended. Bailing out...");
            Assert.isTrue(settings.getMappingId() != null && !StringUtils.EMPTY.equals(settings.getMappingId()), "When using delete operation, the property " + ConfigurationOptions.ES_MAPPING_ID + " must be set and must not be empty since we need the document id in order to delete it. Bailing out...");
        }

        // Check to make sure user doesn't specify more than one script type
        boolean hasScript = false;
        String[] scripts = {settings.getUpdateScriptInline(), settings.getUpdateScriptFile(), settings.getUpdateScriptStored()};
        for (String script : scripts) {
            boolean isSet = StringUtils.hasText(script);
            Assert.isTrue((hasScript && isSet) == false, "Multiple scripts are specified. Please specify only one via [es.update.script.inline], [es.update.script.file], or [es.update.script.stored]");
            hasScript = hasScript || isSet;
        }

        // Early attempt to catch the internal field filtering clashing with user specified field filtering
        SettingsUtils.determineSourceFields(settings); // ignore return, just checking for the throw.
    }

    public static void validateSettingsForReading(Settings settings) {
        checkIndexNameForRead(settings);
        checkIndexStatus(settings);
    }

    public static void validateSettingsForWriting(Settings settings) {
        EsMajorVersion version = settings.getInternalVersionOrThrow();

        // Things that were removed in 6.x and forward
        if (version.onOrAfter(EsMajorVersion.V_6_X)) {
            // File Scripts
            if (StringUtils.hasText(settings.getUpdateScriptFile())) {
                throw new EsHadoopIllegalArgumentException("Cannot use file scripts on ES 6.x and above. Please use " +
                        "stored scripts with [" + ConfigurationOptions.ES_UPDATE_SCRIPT_STORED + "] instead.");
            }

            // Timestamp and TTL in index/updates
            if (StringUtils.hasText(settings.getMappingTimestamp())) {
                throw new EsHadoopIllegalArgumentException("Cannot use timestamps on index/update requests in ES 6.x " +
                        "and above. Please remove the [" + ConfigurationOptions.ES_MAPPING_TIMESTAMP + "] setting.");
            }
            if (StringUtils.hasText(settings.getMappingTtl())) {
                throw new EsHadoopIllegalArgumentException("Cannot use TTL on index/update requests in ES 6.x and " +
                        "above. Please remove the [" + ConfigurationOptions.ES_MAPPING_TTL + "] setting.");
            }
        } else {
            if (StringUtils.hasText(settings.getMappingTtl())) {
                LOG.warn("Setting [" + ConfigurationOptions.ES_MAPPING_TTL + "] is deprecated! Support for [ttl] on " +
                        "indexing and update requests has been removed in ES 6.x and above!");
            }
            if (StringUtils.hasText(settings.getMappingTimestamp())) {
                LOG.warn("Setting [" + ConfigurationOptions.ES_MAPPING_TIMESTAMP + "] is deprecated! Support for " +
                        "[timestamp] on indexing and update requests has been removed in ES 6.x and above!");
            }

        }
    }

    /**
     * Retrieves the Elasticsearch cluster name and version from the settings, or, if they should be missing,
     * creates a bootstrap client and obtains their values.
     */
    public static ClusterInfo discoverClusterInfo(Settings settings, Log log) {
        ClusterName remoteClusterName = null;
        EsMajorVersion remoteVersion = null;
        String clusterName = settings.getProperty(InternalConfigurationOptions.INTERNAL_ES_CLUSTER_NAME);
        String clusterUUID = settings.getProperty(InternalConfigurationOptions.INTERNAL_ES_CLUSTER_UUID);
        String version = settings.getProperty(InternalConfigurationOptions.INTERNAL_ES_VERSION);
        if (StringUtils.hasText(clusterName) && StringUtils.hasText(version)) { // UUID is optional for now
            if (log.isDebugEnabled()) {
                log.debug(String.format("Elasticsearch cluster [NAME:%s][UUID:%s][VERSION:%s] already present in configuration; skipping discovery",
                        clusterName, clusterUUID, version));
            }
            remoteClusterName = new ClusterName(clusterName, clusterUUID);
            remoteVersion = EsMajorVersion.parse(version);
            return new ClusterInfo(remoteClusterName, remoteVersion);
        }

        RestClient bootstrap = new RestClient(settings);
        // first get ES main action info
        try {
            ClusterInfo mainInfo = bootstrap.mainInfo();
            if (log.isDebugEnabled()) {
                log.debug(String.format("Discovered Elasticsearch cluster [%s/%s], version [%s]",
                        mainInfo.getClusterName().getName(),
                        mainInfo.getClusterName().getUUID(),
                        mainInfo.getMajorVersion()));
            }
            settings.setInternalClusterInfo(mainInfo);
            return mainInfo;
        } catch (EsHadoopException ex) {
            throw new EsHadoopIllegalArgumentException(String.format("Cannot detect ES version - "
                    + "typically this happens if the network/Elasticsearch cluster is not accessible or when targeting "
                    + "a WAN/Cloud instance without the proper setting '%s'", ConfigurationOptions.ES_NODES_WAN_ONLY), ex);
        } finally {
            bootstrap.close();
        }
    }

    /**
     * @deprecated Use {@link InitializationUtils#discoverClusterInfo(Settings, Log)} instead
     */
    @Deprecated
    public static EsMajorVersion discoverEsVersion(Settings settings, Log log) {
        return discoverClusterInfo(settings, log).getMajorVersion();
    }

    public static void checkIndexExistence(RestRepository client) {
        if (!client.getSettings().getIndexAutoCreate()) {
            doCheckIndexExistence(client.getSettings(), client);
        }
    }

    public static void checkIndexExistence(Settings settings) {
        // Only open a connection and check if autocreate is disabled
        if (!settings.getIndexAutoCreate()) {
            RestRepository repository = new RestRepository(settings);
            try {
                doCheckIndexExistence(settings, repository);
            } finally {
                repository.close();
            }
        }
    }

    private static void doCheckIndexExistence(Settings settings, RestRepository client) {
        // check index existence
        if (!client.resourceExists(false)) {
            throw new EsHadoopIllegalArgumentException(String.format("Target index [%s] does not exist and auto-creation is disabled [setting '%s' is '%s']",
                    settings.getResourceWrite(), ConfigurationOptions.ES_INDEX_AUTO_CREATE, settings.getIndexAutoCreate()));
        }
    }
    
    public static boolean setMetadataExtractorIfNotSet(Settings settings, Class<? extends MetadataExtractor> clazz, Log log) {
        if (!StringUtils.hasText(settings.getMappingMetadataExtractorClassName())) {
            Log logger = (log != null ? log : LogFactory.getLog(clazz));

            String name = clazz.getName();
            settings.setProperty(ConfigurationOptions.ES_MAPPING_METADATA_EXTRACTOR_CLASS, name);
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("Using pre-defined metadata extractor [%s] as default", settings.getMappingMetadataExtractorClassName()));
            }
            return true;
        }

        return false;
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

    public static boolean setUserProviderIfNotSet(Settings settings, Class<? extends UserProvider> clazz, Log log) {
        if (!StringUtils.hasText(settings.getSecurityUserProviderClass())) {
            settings.setProperty(ConfigurationOptions.ES_SECURITY_USER_PROVIDER_CLASS, clazz.getName());
            Log logger = (log != null ? log : LogFactory.getLog(clazz));
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("Using pre-defined user provider [%s] as default", settings.getSecurityUserProviderClass()));
            }
            return true;
        }
        return false;
    }
}
