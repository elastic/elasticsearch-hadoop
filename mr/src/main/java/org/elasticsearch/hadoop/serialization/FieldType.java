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

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public enum FieldType {
    // core Types
    NULL,
    BOOLEAN,
    BYTE,
    SHORT,
    INTEGER,
    LONG,
    FLOAT,
    DOUBLE,
    STRING,
    DATE,
    DATE_NANOS,
    BINARY,
    TOKEN_COUNT,
    // ES 5.x
    TEXT, KEYWORD, HALF_FLOAT, SCALED_FLOAT,
    WILDCARD,

    GEO_POINT,
    GEO_SHAPE,

    // compound types
    OBJECT,
    NESTED,
    JOIN,

    // not supported yet
    IP,

    // ignored
    COMPLETION;

    private static final Set<String> KNOWN_TYPES = new LinkedHashSet<String>();
    private static final Map<FieldType, LinkedHashSet<FieldType>> CAST_HIERARCHY = new HashMap<FieldType, LinkedHashSet<FieldType>>();

    static {
        for (FieldType fieldType : EnumSet.allOf(FieldType.class)) {
            KNOWN_TYPES.add(fieldType.name());
        }
        CAST_HIERARCHY.put(NULL,             new LinkedHashSet<FieldType>());
        CAST_HIERARCHY.put(BOOLEAN,          new LinkedHashSet<FieldType>(Collections.singletonList(KEYWORD)));
        CAST_HIERARCHY.put(LONG,             new LinkedHashSet<FieldType>(Collections.singletonList(KEYWORD)));
        CAST_HIERARCHY.put(INTEGER,          new LinkedHashSet<FieldType>(Arrays.asList(LONG, KEYWORD)));
        CAST_HIERARCHY.put(SHORT,            new LinkedHashSet<FieldType>(Arrays.asList(INTEGER, LONG, KEYWORD)));
        CAST_HIERARCHY.put(BYTE,             new LinkedHashSet<FieldType>(Arrays.asList(SHORT, INTEGER, LONG, KEYWORD)));
        CAST_HIERARCHY.put(DOUBLE,           new LinkedHashSet<FieldType>(Collections.singletonList(KEYWORD)));
        CAST_HIERARCHY.put(FLOAT,            new LinkedHashSet<FieldType>(Arrays.asList(DOUBLE, KEYWORD)));
        CAST_HIERARCHY.put(STRING,           new LinkedHashSet<FieldType>(Collections.singletonList(KEYWORD)));
        CAST_HIERARCHY.put(DATE,             new LinkedHashSet<FieldType>(Collections.singletonList(KEYWORD)));
        CAST_HIERARCHY.put(DATE_NANOS,       new LinkedHashSet<FieldType>(Collections.singletonList(KEYWORD)));
        CAST_HIERARCHY.put(BINARY,           new LinkedHashSet<FieldType>(Collections.singletonList(KEYWORD)));
        CAST_HIERARCHY.put(TOKEN_COUNT,      new LinkedHashSet<FieldType>(Arrays.asList(LONG, KEYWORD)));
        CAST_HIERARCHY.put(TEXT,             new LinkedHashSet<FieldType>(Collections.singletonList(KEYWORD)));
        CAST_HIERARCHY.put(KEYWORD,          new LinkedHashSet<FieldType>());
        CAST_HIERARCHY.put(WILDCARD,         new LinkedHashSet<FieldType>(Collections.singletonList(KEYWORD)));
        CAST_HIERARCHY.put(HALF_FLOAT,       new LinkedHashSet<FieldType>(Arrays.asList(FLOAT, DOUBLE, KEYWORD)));
        CAST_HIERARCHY.put(SCALED_FLOAT,     new LinkedHashSet<FieldType>(Arrays.asList(DOUBLE, KEYWORD)));
        CAST_HIERARCHY.put(GEO_POINT,        new LinkedHashSet<FieldType>());
        CAST_HIERARCHY.put(GEO_SHAPE,        new LinkedHashSet<FieldType>());
        CAST_HIERARCHY.put(OBJECT,           new LinkedHashSet<FieldType>());
        CAST_HIERARCHY.put(NESTED,           new LinkedHashSet<FieldType>());
        CAST_HIERARCHY.put(JOIN,             new LinkedHashSet<FieldType>());
        CAST_HIERARCHY.put(IP,               new LinkedHashSet<FieldType>(Collections.singletonList(KEYWORD)));
        CAST_HIERARCHY.put(COMPLETION,       new LinkedHashSet<FieldType>());
    }

    public static FieldType parse(String name) {
        if (name == null) {
            return null;
        }
        String n = name.toUpperCase(Locale.ENGLISH);
        return (KNOWN_TYPES.contains(n) ? FieldType.valueOf(n) : null);
    }

    public static boolean isRelevant(FieldType fieldType) {
        if (fieldType == null || COMPLETION == fieldType) {
            return false;
        }

        return true;
    }

    /**
     * Compound fields are fields that contain subfields underneath them.
     */
    public static boolean isCompound(FieldType fieldType) {
        return (OBJECT == fieldType || NESTED == fieldType || JOIN == fieldType);
    }

    public static boolean isGeo(FieldType fieldType) {
        return (GEO_POINT == fieldType || GEO_SHAPE == fieldType);
    }

    public LinkedHashSet<FieldType> getCastingTypes() {
        LinkedHashSet<FieldType> types = CAST_HIERARCHY.get(this);
        if (types == null) {
            types = new LinkedHashSet<FieldType>();
        }
        return types;
    }
}