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
package org.elasticsearch.hadoop.serialization.field;

import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.serialization.SettingsAware;
import org.elasticsearch.hadoop.serialization.bulk.RawJson;
import org.elasticsearch.hadoop.util.StringUtils;

public class ConstantFieldExtractor implements FieldExtractor, SettingsAware {

    public static final String PROPERTY = "org.elasticsearch.hadoop.serialization.ConstantFieldExtractor.property";
    private String fieldName;
    private Object value;
	private boolean autoQuote = true;

    @Override
    public final Object field(Object target) {
		return (value != null ? value : extractField(target));
    }

    protected Object extractField(Object target) {
        return NOT_FOUND;
    }

    @Override
    public void setSettings(Settings settings) {
		autoQuote = settings.getMappingConstantAutoQuote();
        fieldName = property(settings);
        if (fieldName.startsWith("<") && fieldName.endsWith(">")) {
            value = initValue(fieldName.substring(1, fieldName.length() - 1));
        }
        if (value == null) {
            processField(settings, fieldName);
        }
    }

    protected void processField(Settings settings, String fieldName) {
    }

    protected Object initValue(String value) {
		// check for quote and automatically add them, if needed for non-numbers
		if (autoQuote && !value.startsWith("\"") && !value.endsWith("\"")) {
			// constant values
			if (!("null".equals(value) || "true".equals(value) || "false".equals(value))) {
				// try number parsing
				if (value.startsWith("-")) {
					value = value.substring(1);
				}
				boolean isNumber = true;
				for (int i = 0; i < value.length(); i++) {
					if (!Character.isDigit(value.charAt(i))) {
						isNumber = false;
						break;
					}
				}
				if (!isNumber) {
					value = StringUtils.toJsonString(value);
				}

			}
		}
		return new RawJson(value);
    }

    protected String property(Settings settings) {
        String value = settings.getProperty(PROPERTY);
        return (value == null ? "" : value.trim());
    }

    protected String getFieldName() {
        return fieldName;
    }

    @Override
    public String toString() {
        return String.format("%s for field [%s]", getClass().getSimpleName(), fieldName);
    }
}
