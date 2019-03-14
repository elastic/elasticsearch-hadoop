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

package org.elasticsearch.hadoop.serialization.dto.mapping;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.hadoop.serialization.FieldType;
import org.elasticsearch.hadoop.serialization.field.FieldFilter;

/**
 * A mapping has a name and a collection of fields.
 */
public class Mapping implements Serializable {

    private final String index;
    private final String type;
    private final Field[] fields;

    /**
     * Construct a new mapping
     * @param name The name of the mapping
     * @param fields The fields in the mapping
     */
    public Mapping(String name, Collection<Field> fields) {
        this(null, name, (fields != null ? fields.toArray(new Field[fields.size()]) : Field.NO_FIELDS));
    }

    public Mapping(String index, String name, Collection<Field> fields) {
        this(index, name, (fields != null ? fields.toArray(new Field[fields.size()]) : Field.NO_FIELDS));
    }

    Mapping(String index, String type, Field[] fields) {
        this.index = index;
        this.type = type;
        this.fields = fields;
    }

    public String getIndex() {
        return index;
    }

    public String getType() {
        return type;
    }

    public Field[] getFields() {
        return fields;
    }

    /**
     * Filters out fields based on the provided include and exclude information and returns a Mapping object
     * @param includes Field names to explicitly include in the mapping
     * @param excludes Field names to explicitly exclude in the mapping
     * @return this if no fields were filtered, or a new Mapping with the modified field lists.
     */
    public Mapping filter(Collection<String> includes, Collection<String> excludes) {
        if (includes.isEmpty() && excludes.isEmpty()) {
            return this;
        }

        List<Field> filtered = new ArrayList<Field>();
        List<FieldFilter.NumberedInclude> convertedIncludes = FieldFilter.toNumberedFilter(includes);

        boolean intact = true;
        for (Field fl : this.getFields()) {
            intact &= filterField(fl, null, filtered, convertedIncludes, excludes);
        }

        return (intact ? this : new Mapping(this.getIndex(), this.getType(), filtered));
    }

    private static boolean filterField(Field field, String parentName, List<Field> filtered, Collection<FieldFilter.NumberedInclude> includes, Collection<String> excludes) {
        String fieldName = (parentName != null ? parentName + "." + field.name() : field.name());

        boolean intact = true;

        if (FieldFilter.filter(fieldName, includes, excludes).matched) {
            if (FieldType.isCompound(field.type())) {
                List<Field> nested = new ArrayList<Field>();
                for (Field nestedField : field.properties()) {
                    intact &= filterField(nestedField, fieldName, nested, includes, excludes);
                }
                filtered.add(new Field(field.name(), field.type(), nested));
            }
            else {
                filtered.add(field);
            }
        }
        else {
            intact = false;
        }
        return intact;
    }

    /**
     * Takes a mapping tree and returns a map of all of its fields flattened, and paired with their
     * field types.
     */
    public Map<String, FieldType> flatten() {
        if (fields == null || fields.length == 0) {
            return Collections.<String, FieldType> emptyMap();
        }

        Map<String, FieldType> map = new LinkedHashMap<String, FieldType>();

        for (Field nestedField : fields) {
            addSubFieldToMap(map, nestedField, null);
        }

        return map;
    }

    private static void addSubFieldToMap(Map<String, FieldType> fields, Field field, String parentName) {
        String fieldName = (parentName != null ? parentName + "." + field.name() : field.name());

        fields.put(fieldName, field.type());

        if (FieldType.isCompound(field.type())) {
            for (Field nestedField : field.properties()) {
                addSubFieldToMap(fields, nestedField, fieldName);
            }
        }
    }

    @Override
    public String toString() {
        if (type != MappingSet.TYPELESS_MAPPING_NAME) {
            return String.format("%s/%s=%s", index, type, Arrays.toString(fields));
        } else {
            return String.format("%s=%s", index, Arrays.toString(fields));
        }
    }
}
