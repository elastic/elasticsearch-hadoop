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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.util.StringUtils;

public abstract class SerializationUtils {

    public static boolean setValueWriterIfNotSet(Settings settings, Class<? extends ValueWriter<?>> clazz, Log log) {

        if (!StringUtils.hasText(settings.getSerializerValueWriterClassName())) {
            settings.setProperty(ConfigurationOptions.ES_SERIALIZATION_WRITER_CLASS, clazz.getName());
            Log logger = (log != null ? log : LogFactory.getLog(clazz));
            logger.debug(String.format("Using pre-defined writer serializer [%s] as default", settings.getSerializerValueWriterClassName()));
            return true;
        }

        return false;
    }

    public static boolean setValueReaderIfNotSet(Settings settings, Class<? extends ValueReader> clazz, Log log) {

        if (!StringUtils.hasText(settings.getSerializerValueReaderClassName())) {
            settings.setProperty(ConfigurationOptions.ES_SERIALIZATION_READER_CLASS, clazz.getName());
            Log logger = (log != null ? log : LogFactory.getLog(clazz));
            logger.debug(String.format("Using pre-defined reader serializer [%s] as default", settings.getSerializerValueReaderClassName()));
            return true;
        }

        return false;
    }
}
