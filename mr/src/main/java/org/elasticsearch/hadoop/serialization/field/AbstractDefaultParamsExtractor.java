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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.serialization.SettingsAware;
import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.StringUtils;

public abstract class AbstractDefaultParamsExtractor implements FieldExtractor, SettingsAware, FieldExplainer, WithoutQuotes {

    private Map<String, FieldExtractor> params = new LinkedHashMap<String, FieldExtractor>();
    protected Settings settings;
    // field explainer saved in case of a failure for diagnostics
    private FieldExtractor lastFailingFieldExtractor;

    @Override
    public Object field(Object target) {
        List<Object> list = new ArrayList<Object>(params.size());
        for (Entry<String, FieldExtractor> entry : params.entrySet()) {
            list.add("\"");
            list.add(entry.getKey());
            list.add("\":");
            Object field = entry.getValue().field(target);
            if (field == FieldExtractor.NOT_FOUND) {
                lastFailingFieldExtractor = entry.getValue();
                return FieldExtractor.NOT_FOUND;
            }
            list.add(field);
            list.add(",");
        }
        list.remove(list.size() - 1);
        return list;
    }

    @Override
    public String toString(Object target) {
        return (lastFailingFieldExtractor instanceof FieldExplainer ? ((FieldExplainer) lastFailingFieldExtractor).toString(target) : target.toString());
    }

    @Override
    public void setSettings(Settings settings) {
        this.settings = settings;

        String paramString = settings.getUpdateScriptParams();
        List<String> fields = StringUtils.tokenize(paramString);
        for (String string : fields) {
            List<String> param = StringUtils.tokenize(string, ":");
            Assert.isTrue(param.size() == 2, "Invalid param definition " + string);

            params.put(param.get(0), createFieldExtractor(param.get(1)));
        }
    }

    @Override
    public String toString() {
        if (lastFailingFieldExtractor != null) {
            return lastFailingFieldExtractor.toString();
        }

        return String.format("%s for fields [%s]", getClass().getSimpleName(), params.keySet());
    }

    protected abstract FieldExtractor createFieldExtractor(String fieldName);
}
