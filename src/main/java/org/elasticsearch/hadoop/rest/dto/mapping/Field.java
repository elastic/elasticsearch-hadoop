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
package org.elasticsearch.hadoop.rest.dto.mapping;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.elasticsearch.hadoop.serialization.FieldType;

@SuppressWarnings("serial")
public class Field implements Serializable {

    private final String name;
    private final FieldType type;
    private final Field[] properties;

    public Field(String name, FieldType type) {
        this(name, type, (Field[]) null);
    }

    public Field(String name, FieldType type, Collection<Field> properties) {
        this(name, type, (properties != null ? properties.toArray(new Field[properties.size()]) : null));
    }

    Field(String name, FieldType type, Field[] properties) {
        this.name = name;
        this.type = type;
        this.properties = properties;
    }

    public Field[] properties() { return properties; }

    public FieldType type() {
        return type;
    }

    public String name() { return name; }

    public static Field parseField(Map<String, Object> content) {
        return parseField(content.entrySet().iterator().next(), null);
    }

    public static Map<String, FieldType> toLookupMap(Field field) {
        if (field == null) {
            return Collections.<String, FieldType> emptyMap();
        }

        Map<String, FieldType> map = new LinkedHashMap<String, FieldType>();

        for (Field nestedField : field.properties()) {
            add(map, nestedField, null);
        }

        return map;
    }

    static void add(Map<String, FieldType> fields, Field field, String parentName) {
        if (FieldType.OBJECT == field.type()) {
            if (parentName != null) {
                parentName = parentName + "." + field.name();
            }
            for (Field nestedField : field.properties()) {
                add(fields, nestedField, parentName);
            }
        }
        else {
            fields.put(field.name(), field.type());
        }
    }

    @SuppressWarnings("unchecked")
    private static Field parseField(Entry<String, Object> entry, String previousKey) {
        // can be "type" or field name
        String key = entry.getKey();
        Object value = entry.getValue();

        // nested object
        if (value instanceof Map) {
            Map<String, Object> content = (Map<String, Object>) value;

            // check type first
            Object type = content.get("type");
            if (type instanceof String) {
                String typeString = type.toString();
                FieldType fieldType = FieldType.parse(typeString);

                // handle multi_field separately
                if (FieldType.MULTI_FIELD == fieldType) {
                    // get fields
                    Map<String, Object> fields = (Map<String, Object>) content.get("fields");
                    // return default field
                    Map<String, Object> defaultField = (Map<String, Object>) fields.get(key);
                    return new Field(key, FieldType.parse(defaultField.get("type").toString()));
                }

                if (FieldType.isRelevant(fieldType)) {
                    return new Field(key, fieldType);
                }
                else {
                    return null;
                }
            }

            // no type - iterate through types
            List<Field> fields = new ArrayList<Field>(content.size());
            for (Entry<String, Object> e : content.entrySet()) {
                if (e.getValue() instanceof Map) {
                    Field fl = parseField(e, key);
                    if (fl != null && fl.type == FieldType.OBJECT && "properties".equals(fl.name)) {
                        return new Field(key, fl.type, fl.properties);
                    }
                    if (fl != null) {
                        fields.add(fl);
                    }
                }
            }
            return new Field(key, FieldType.OBJECT, fields);
        }


        throw new IllegalArgumentException("invalid map received " + entry);
    }

    public String toString() {
        return String.format("%s=%s", name, (type == FieldType.OBJECT ? Arrays.toString(properties) : type));
    }
}