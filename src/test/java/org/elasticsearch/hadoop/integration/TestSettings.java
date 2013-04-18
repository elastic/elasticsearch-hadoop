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
package org.elasticsearch.hadoop.integration;

import java.util.Properties;

import org.elasticsearch.hadoop.cfg.PropertiesSettings;

/**
 * Tweaked settings for testing.
 */
public class TestSettings extends PropertiesSettings {

    public final static Properties TESTING_PROPS = new Properties();

    static {
        TESTING_PROPS.put(ES_BATCH_SIZE_BYTES, "8kb");
        // see TestSettings
        TESTING_PROPS.put(ES_PORT, "9700");
    }

    public TestSettings() {
        super(TESTING_PROPS);
    }
}
