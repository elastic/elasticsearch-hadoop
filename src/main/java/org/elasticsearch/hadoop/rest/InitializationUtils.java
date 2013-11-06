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
package org.elasticsearch.hadoop.rest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.cfg.SettingsManager;
import org.elasticsearch.hadoop.serialization.ContentBuilder;
import org.elasticsearch.hadoop.serialization.FieldExtractor;
import org.elasticsearch.hadoop.serialization.ValueWriter;
import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.BytesArray;
import org.elasticsearch.hadoop.util.FastByteArrayOutputStream;
import org.elasticsearch.hadoop.util.StringUtils;

public abstract class InitializationUtils {

    public static void checkIdForOperation(Settings settings) {
        String operation = settings.getOperation();

        if (ConfigurationOptions.ES_OPERATION_UPDATE.equals(operation)) {
            Assert.isTrue(StringUtils.hasText(settings.getMappingId()),
                    String.format("Operation [%s] requires an id but none (%s) was specified", operation, ConfigurationOptions.ES_MAPPING_ID));
        }
    }

    public static void checkIndexExistence(Settings settings, BufferedRestClient client) {
        // check index existence
        if (!settings.getIndexAutoCreate()) {
            if (client == null) {
                client = new BufferedRestClient(settings);
            }
            if (!client.indexExists()) {
                client.close();
                throw new IllegalArgumentException(String.format("Target index [%s] does not exist and auto-creation is disabled [setting '%s' is '%s']",
                        settings.getTargetResource(), ConfigurationOptions.ES_INDEX_AUTO_CREATE, settings.getIndexAutoCreate()));
            }
        }
    }

    public static boolean setFieldExtractorIfNotSet(Settings settings, Class<? extends FieldExtractor> clazz, Log log) {
        if (!StringUtils.hasText(settings.getMappingIdExtractorClassName())) {
            settings.setProperty(ConfigurationOptions.ES_MAPPING_DEFAULT_EXTRACTOR_CLASS, clazz.getName());
            Log logger = (log != null ? log : LogFactory.getLog(clazz));
            logger.debug(String.format("Using pre-defined field extractor [%s] as default", settings.getMappingIdExtractorClassName()));
            return true;
        }

        return false;
    }

    public static <T> void saveSchemaIfNeeded(Object conf, ValueWriter<T> schemaWriter, T schema, Log log) {
        Settings settings = SettingsManager.loadFrom(conf);

        if (settings.getIndexAutoCreate()) {
            BufferedRestClient client = new BufferedRestClient(settings);
            if (!client.indexExists()) {
                if (schemaWriter == null) {
                    log.warn(String.format("No mapping found [%s] and no schema found; letting Elasticsearch perform auto-mapping...",  settings.getTargetResource()));
                }
                else {
                    log.info(String.format("No mapping found [%s], creating one based on given schema", settings.getTargetResource()));
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
}
