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

package org.elasticsearch.hadoop.handler.impl.elasticsearch;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.hadoop.cfg.CompositeSettings;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.cfg.InternalConfigurationOptions;
import org.elasticsearch.hadoop.cfg.PropertiesSettings;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.handler.ErrorCollector;
import org.elasticsearch.hadoop.handler.ErrorHandler;
import org.elasticsearch.hadoop.handler.Exceptional;
import org.elasticsearch.hadoop.handler.HandlerResult;
import org.elasticsearch.hadoop.rest.InitializationUtils;
import org.elasticsearch.hadoop.rest.Resource;
import org.elasticsearch.hadoop.rest.RestClient;
import org.elasticsearch.hadoop.rest.RestRepository;
import org.elasticsearch.hadoop.rest.RestService;
import org.elasticsearch.hadoop.serialization.field.IndexExtractor;
import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.BytesArray;
import org.elasticsearch.hadoop.util.ObjectUtils;
import org.elasticsearch.hadoop.util.SettingsUtils;
import org.elasticsearch.hadoop.util.StringUtils;
import org.elasticsearch.hadoop.util.ecs.ElasticCommonSchema;
import org.elasticsearch.hadoop.util.ecs.ElasticCommonSchema.TemplateBuilder;
import org.elasticsearch.hadoop.util.ecs.MessageTemplate;
import org.elasticsearch.hadoop.util.unit.Booleans;

/**
 * Generic Error Handler that converts error events into JSON documents, and stores them in an Elasticsearch index.
 * <p>
 * Handler results returned from this handler are configurable; In the case that the event is successfully written to
 * Elasticsearch it returns HANDLED by default and if the event cannot be written for any reason it returns ABORT by
 * default.
 * <p>
 *
 * @param <I> type of error event
 * @param <O> in case of retries, this is the type of the retry value
 * @param <C> the type of error collector used
 */
public class ElasticsearchHandler<I extends Exceptional, O, C extends ErrorCollector<O>> implements ErrorHandler<I, O, C> {

    private static final Log LOG = LogFactory.getLog(ElasticsearchHandler.class);

    private static final String CONST_EVENT_CATEGORY = "error";

    // Config Names
    /// Return logic: Use "return".<case>[.reason]
    public static final String CONF_RETURN_VALUE = "return.default";
    public static final String CONF_RETURN_VALUE_DEFAULT = HandlerResult.HANDLED.toString();
    public static final String CONF_RETURN_ERROR = "return.error";
    public static final String CONF_RETURN_ERROR_DEFAULT = HandlerResult.ABORT.toString();
    public static final String CONF_PASS_REASON_SUFFIX = "reason";

    /// Document metadata
    public static final String CONF_LABEL = "label";
    public static final String CONF_TAGS = "tags";

    /// Client configuration
    public static final String CONF_CLIENT_NODES = "client.nodes";
    public static final String CONF_CLIENT_PORT = "client.port";
    public static final String CONF_CLIENT_RESOURCE = "client.resource";
    public static final String CONF_CLIENT_INHERIT= "client.inherit";
    public static final String CONF_CLIENT_CONF = "client.conf";

    // Settings
    private HandlerResult returnDefault;
    private String successReason;
    private HandlerResult returnError;
    private String errorReason;

    // State
    private Settings rootSettings;
    private Settings clientSettings;
    private EventConverter<I> eventConverter;
    private MessageTemplate messageTemplate;
    private boolean initialized;
    private Resource endpoint;
    private RestRepository writeClient;

    public static <I extends Exceptional, O, C extends ErrorCollector<O>> ElasticsearchHandler<I, O, C> create(Settings rootSettings, EventConverter<I> converter) {
        return new ElasticsearchHandler<I, O, C>(rootSettings, converter);
    }

    public ElasticsearchHandler(Settings rootSettings, EventConverter<I> eventConverter) {
        this.rootSettings = rootSettings;
        this.eventConverter = eventConverter;
    }

    @Override
    public void init(Properties properties) {
        // Collect Handler settings
        Settings handlerSettings = new PropertiesSettings(properties);
        boolean inheritRoot = true;
        if (handlerSettings.getProperty(CONF_CLIENT_INHERIT) != null) {
            inheritRoot = Booleans.parseBoolean(handlerSettings.getProperty(CONF_CLIENT_INHERIT));
        }

        // Exception: Should persist transport pooling key if it is preset in the root config, regardless of the inherit settings.
        if (SettingsUtils.hasJobTransportPoolingKey(rootSettings)) {
            String jobKey = SettingsUtils.getJobTransportPoolingKey(rootSettings);
            // We want to use a different job key based on the current one since error handlers might write
            // to other clusters or have seriously different settings from the current rest client.
            String newJobKey = jobKey + "_" + UUID.randomUUID().toString();
            // Place under the client configuration for the handler
            handlerSettings.setProperty(CONF_CLIENT_CONF + "." + InternalConfigurationOptions.INTERNAL_TRANSPORT_POOLING_KEY, newJobKey);
        }

        // Gather high level configs and push to client conf level
        resolveProperty(CONF_CLIENT_NODES, CONF_CLIENT_CONF + "." + ConfigurationOptions.ES_NODES, handlerSettings);
        resolveProperty(CONF_CLIENT_PORT, CONF_CLIENT_CONF + "." + ConfigurationOptions.ES_PORT, handlerSettings);
        resolveProperty(CONF_CLIENT_RESOURCE, CONF_CLIENT_CONF + "." + ConfigurationOptions.ES_RESOURCE_WRITE, handlerSettings);
        resolveProperty(CONF_CLIENT_RESOURCE, CONF_CLIENT_CONF + "." + ConfigurationOptions.ES_RESOURCE, handlerSettings);

        // Inherit the original configuration or not
        this.clientSettings = handlerSettings.getSettingsView(CONF_CLIENT_CONF);

        // Ensure we have a write resource to use
        Assert.hasText(clientSettings.getResourceWrite(), "Could not locate write resource for ES error handler.");

        if (inheritRoot) {
            LOG.info("Elasticsearch Error Handler inheriting root configuration");
            this.clientSettings = new CompositeSettings(Arrays.asList(clientSettings, rootSettings.excludeFilter("es.internal")));
        } else {
            LOG.info("Elasticsearch Error Handler proceeding without inheriting root configuration options as configured");
        }

        // Ensure no pattern in Index format, and extract the index to send errors to
        InitializationUtils.discoverAndValidateClusterInfo(clientSettings, LOG);
        Resource resource = new Resource(clientSettings, false);
        IndexExtractor iformat = ObjectUtils.instantiate(clientSettings.getMappingIndexExtractorClassName(), handlerSettings);
        iformat.compile(resource.toString());
        if (iformat.hasPattern()) {
            throw new IllegalArgumentException(String.format("Cannot use index format within Elasticsearch Error Handler. Format was [%s]", resource.toString()));
        }
        this.endpoint = resource;

        // Configure ECS
        ElasticCommonSchema schema = new ElasticCommonSchema();
        TemplateBuilder templateBuilder = schema.buildTemplate()
                .setEventCategory(CONST_EVENT_CATEGORY);

        // Add any Labels and Tags to schema
        for (Map.Entry entry: handlerSettings.getSettingsView(CONF_LABEL).asProperties().entrySet()) {
            templateBuilder.addLabel(entry.getKey().toString(), entry.getValue().toString());
        }
        templateBuilder.addTags(StringUtils.tokenize(handlerSettings.getProperty(CONF_TAGS)));

        // Configure template using event handler
        templateBuilder = eventConverter.configureTemplate(templateBuilder);
        this.messageTemplate = templateBuilder.build();

        // Determine the behavior for successful write and error on write:
        this.returnDefault = HandlerResult.valueOf(handlerSettings.getProperty(CONF_RETURN_VALUE, CONF_RETURN_VALUE_DEFAULT));
        if (HandlerResult.PASS == returnDefault) {
            this.successReason = handlerSettings.getProperty(CONF_RETURN_VALUE + "." + CONF_PASS_REASON_SUFFIX);
        } else {
            this.successReason = null;
        }
        this.returnError = HandlerResult.valueOf(handlerSettings.getProperty(CONF_RETURN_ERROR, CONF_RETURN_ERROR_DEFAULT));
        if (HandlerResult.PASS == returnError) {
            this.errorReason = handlerSettings.getProperty(CONF_RETURN_ERROR + "." + CONF_PASS_REASON_SUFFIX);
        } else {
            this.errorReason = null;
        }
    }

    private void resolveProperty(String highLevelProperty, String explicitProperty, Settings subject) {
        String confValue = subject.getProperty(highLevelProperty);
        String explicitValue = subject.getProperty(explicitProperty);
        if (StringUtils.hasText(confValue) && StringUtils.hasText(explicitValue)) {
            LOG.warn(
                    String.format("Found both [%s] and [%s] settings during elasticsearch handler init. Continuing " +
                            "with value from [%s] (%s)",
                            highLevelProperty,
                            explicitProperty,
                            highLevelProperty,
                            confValue
                    )
            );
        }
        if (StringUtils.hasText(confValue)) {
            subject.setProperty(explicitProperty, confValue);
        }
    }

    private void lazyInitWrite() {
        if (!initialized) {
            this.initialized = true;
            this.writeClient = RestService.createWriter(clientSettings, -1, 0, LOG).repository;
        }
    }

    @Override
    public HandlerResult onError(I entry, C collector) throws Exception {
        HandlerResult result;
        try {
            lazyInitWrite();
            if (isOpen()) {
                putDocument(writeClient.getRestClient(), createErrorDocument(entry));
                result = generateResult(returnDefault, successReason, collector);
            } else {
                result = generateResult(returnError, errorReason, collector);
            }
        } catch (Exception e) {
            LOG.error("Could not send error handling data to ES", e);
            result = generateResult(returnError, errorReason, collector);
        }
        return result;
    }

    private boolean isOpen() {
        return writeClient != null;
    }

    private BytesArray createErrorDocument(I entry) throws IOException {
        return eventConverter.generateEvent(entry, messageTemplate);
    }

    private void putDocument(RestClient client, BytesArray document) throws IOException {
        client.postDocument(endpoint, document);
    }

    private HandlerResult generateResult(HandlerResult expectedResult, String possiblePassReason, C collector) {
        if (HandlerResult.PASS == expectedResult) {
            return collector.pass(possiblePassReason);
        } else {
            return expectedResult;
        }
    }

    @Override
    public void close() {
        if (isOpen()) {
            // TODO: look at collecting these stats some other way later.
            if (clientSettings.getBatchRefreshAfterWrite()) {
                writeClient.getRestClient().refresh(endpoint);
            }
            writeClient.close();
        }
    }
}
