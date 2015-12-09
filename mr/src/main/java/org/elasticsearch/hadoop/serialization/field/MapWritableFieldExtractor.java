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

import org.apache.hadoop.io.Text;
import org.elasticsearch.hadoop.cfg.Settings;

public class MapWritableFieldExtractor extends ConstantFieldExtractor implements FieldExplainer {

    private List<Text> fieldNames;

    @SuppressWarnings("rawtypes")
    @Override
    protected Object extractField(Object target) {
        for (int i = 0; i < fieldNames.size(); i++) {
            if (target instanceof Map) {
                Map map = (Map) target;
                if (map.containsKey(fieldNames.get(i))) {
                    target = map.get(fieldNames.get(i));
                }
                else {
                    return NOT_FOUND;
                }
            }
            else {
                return NOT_FOUND;
            }
        }
        return target;
    }

    @Override
    protected void processField(Settings settings, List<String> fldNames) {
        fieldNames = new ArrayList<Text>(fldNames.size());
        for (String string : fldNames) {
            fieldNames.add(new Text(string));
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public String toString(Object field) {
        if (field instanceof Map) {
            return new LinkedHashMap((Map) field).toString();
        }
        return field.toString();
    }
}
