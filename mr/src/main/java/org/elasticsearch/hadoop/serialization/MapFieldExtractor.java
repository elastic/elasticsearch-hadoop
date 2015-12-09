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

import java.util.List;
import java.util.Map;

import org.elasticsearch.hadoop.serialization.field.ConstantFieldExtractor;

public class MapFieldExtractor extends ConstantFieldExtractor {

    @SuppressWarnings("rawtypes")
    @Override
    protected Object extractField(Object target) {
        List<String> fieldNames = getFieldNames();
        for (int i = 0; i < fieldNames.size(); i++) {
            String field = fieldNames.get(i);
            if (target instanceof Map) {
                Map map = (Map) target;
                if (map.containsKey(field)) {
                    target = map.get(field);
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
}
