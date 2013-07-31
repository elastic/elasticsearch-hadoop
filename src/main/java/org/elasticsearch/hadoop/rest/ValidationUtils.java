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

import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.cfg.Settings;

public abstract class ValidationUtils {

    public static void checkIndexExistence(Settings settings, BufferedRestClient client) {
        // check index existence
        if (!settings.getIndexAutoCreate()) {
            if (client == null) {
                client = new BufferedRestClient(settings);
            }
            if (!client.indexExists()) {
                client.close();
                throw new IllegalArgumentException(String.format(
                        "Target index [%s] does not exist and auto-creation disabled [setting '%s' is '%s']",
                        settings.getTargetResource(), ConfigurationOptions.ES_INDEX_AUTO_CREATE, settings.getIndexAutoCreate()));
            }
        }
    }
}
