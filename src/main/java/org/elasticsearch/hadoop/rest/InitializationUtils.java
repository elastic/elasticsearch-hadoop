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
import org.apache.hadoop.conf.Configuration;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.cfg.SettingsManager;
import org.elasticsearch.hadoop.serialization.ContentBuilder;
import org.elasticsearch.hadoop.serialization.ValueWriter;
import org.elasticsearch.hadoop.util.BytesArray;
import org.elasticsearch.hadoop.util.FastByteArrayOutputStream;

public abstract class InitializationUtils {

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

    public static <T> void saveSchemaIfNeeded(Configuration conf, ValueWriter<T> schemaWriter, T schema, Log log) {
        Settings settings = SettingsManager.loadFrom(conf);

        if (settings.getIndexAutoCreate()) {
            BufferedRestClient client = new BufferedRestClient(settings);
            if (!client.indexExists()) {
                if (schemaWriter == null) {
                    log.warn(String.format("No mapping found [%s] and no schema found; letting Elasticsearch perform auto-mapping...",  settings.getTargetResource()));
                }
                else {
                    log.info(String.format("No mapping found [%s], creating one based on given schema", settings.getTargetResource()));
                }
            }
            else {
                ContentBuilder builder = ContentBuilder.generate(schemaWriter).value(schema).flush();
                BytesArray content = ((FastByteArrayOutputStream) builder.content()).bytes();
                builder.close();
                client.putMapping(content);
            }
            client.close();
        }
    }
}
