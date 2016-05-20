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
package org.elasticsearch.storm.serialization;

import java.util.List;

import org.elasticsearch.hadoop.serialization.field.ConstantFieldExtractor;
import org.elasticsearch.hadoop.serialization.field.FieldExplainer;

import org.apache.storm.tuple.Tuple;

public class StormTupleFieldExtractor extends ConstantFieldExtractor implements FieldExplainer {

    @Override
    protected Object extractField(Object target) {
        List<String> fieldNames = getFieldNames();
        for (int i = 0; i < fieldNames.size(); i++) {
            String field = fieldNames.get(i);
            if (target instanceof Tuple) {
                target = ((Tuple) target).getValueByField(field);
                if (target == null) {
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
    public String toString(Object field) {
        if (field instanceof Tuple) {
            return ((Tuple) field).getValues().toString();
        }
        return field.toString();
    }
}
