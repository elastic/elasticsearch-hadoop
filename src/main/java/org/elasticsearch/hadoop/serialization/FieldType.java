package org.elasticsearch.hadoop.serialization;

import java.util.EnumSet;
import java.util.LinkedHashSet;
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
    BINARY,

    // compound types
    OBJECT,
    MULTI_FIELD,

    NESTED,

    // not supported yet
    IP,

    GEO_POINT,
    GEO_SHAPE,
    POINT,
    LINESTRING,
    POLYGON,
    MULTIPOINT,
    MULTIPOLYGON,
    ENVELOPE,

    // ignored
    COMPLETION;

    private static final Set<String> KNOWN_TYPES = new LinkedHashSet<String>();

    static {
        for (FieldType fieldType : EnumSet.allOf(FieldType.class)) {
            KNOWN_TYPES.add(fieldType.name());
        }
    }

    public static FieldType parse(String name) {
        String n = (name != null ? name.toUpperCase() : name);
        return (KNOWN_TYPES.contains(n) ? FieldType.valueOf(n) : null);
    }

    public static boolean isRelevant(FieldType fieldType) {
        if (fieldType == null || COMPLETION == fieldType) {
            return false;
        }

        if (IP == fieldType || NESTED == fieldType ||
                GEO_POINT == fieldType || GEO_SHAPE == fieldType ||
                POINT == fieldType || LINESTRING == fieldType || POLYGON == fieldType ||
                MULTIPOINT == fieldType || MULTIPOLYGON == fieldType || ENVELOPE == fieldType) {
            throw new UnsupportedOperationException("field " + fieldType + " not supported yet");
        }

        return true;
    }
}