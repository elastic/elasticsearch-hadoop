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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class Field implements Serializable {



    private final String name;
    private final FieldType type;
    private final Field[] properties;

    public Field(String name, FieldType type) {
        this(name, type, null);
    }

    public Field(String name, FieldType type, Collection<Field> properties) {
        this.name = name;
        this.type = type;
        this.properties = (properties != null ? (Field[]) properties.toArray() : null);
    }

    public Field[] properties() { return properties; }

    public FieldType type() {
        return type;
    }

    public String name() { return name; }

    public static Field parseField(Map<String, Object> content) {
        return parseField(content.entrySet().iterator().next());
    }

    private static Field parseField(Entry<String, Object> entry) {
        String name = entry.getKey();
        Object value = entry.getValue();

        Map<String, Object> content = (Map<String, Object>) value;
        if ("properties".equals(name)) {
            List<Field> fields = new ArrayList<Field>(content.size());
            for (Entry<String, Object> e : content.entrySet()) {
                fields.add(parseField(e));
            }
            return new Field(name, FieldType.OBJECT, fields);
        }
        else {
            return new Field(name, FieldType.parse(content.get("type").toString().toUpperCase()));
        }
    }

    public String toString() {
        return (type == FieldType.OBJECT ? properties.toString() : String.format("%s=%s", name, type));
    }
}