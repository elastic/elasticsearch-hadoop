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
package org.elasticsearch.hadoop.serialization;

import org.elasticsearch.hadoop.cfg.Settings;

public class ConstantFieldExtractor implements FieldExtractor, SettingsAware {

    public static final String PROPERTY = "org.elasticsearch.hadoop.serialization.ConstantFieldExtractor.property";
    private String fieldName;
    private String value;

    @Override
    public final String field(Object target) {
        return (value != null ? value : extractField(target));
    }

    protected String extractField(Object target) {
        return null;
    }

    @Override
    public void setSettings(Settings settings) {
        fieldName = property(settings);
        if (fieldName.startsWith("<") && fieldName.endsWith(">")) {
            this.value = fieldName.substring(1, fieldName.length() - 1);
        }
    }

    protected String property(Settings settings) {
        String property = settings.getProperty(PROPERTY).trim();
        String value = settings.getProperty(property);
        return (value == null ? "" : value.trim());
    }

    protected String getFieldName() {
        return fieldName;
    }
}
