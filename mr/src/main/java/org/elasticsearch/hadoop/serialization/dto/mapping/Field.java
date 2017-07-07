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
import java.util.Arrays;
import java.util.Collection;

import org.elasticsearch.hadoop.serialization.FieldType;

@SuppressWarnings("serial")
public class Field implements Serializable {

    static final Field[] NO_FIELDS = new Field[0];

    private final String name;
    private final FieldType type;
    private final Field[] properties;

    public Field(String name, FieldType type) {
        this(name, type, NO_FIELDS);
    }

    public Field(String name, FieldType type, Collection<Field> properties) {
        this(name, type, (properties != null ? properties.toArray(new Field[properties.size()]) : NO_FIELDS));
    }

    Field(String name, FieldType type, Field[] properties) {
        this.name = name;
        this.type = type;
        this.properties = properties;
    }

    public Field[] properties() {
        return properties;
    }

    public FieldType type() {
        return type;
    }

    public String name() {
        return name;
    }

    @Override
    public String toString() {
        return String.format("%s=%s", name, ((type == FieldType.OBJECT || type == FieldType.NESTED) ? Arrays.toString(properties) : type));
    }
}