package org.elasticsearch.hadoop.serialization;

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
    NESTED,

    // not supported yet
    IP,
    GEO_POINT,
    GEO_SHAPE,

    // ignored
    MULTI_FIELD,
    COMPLETION;

    public static FieldType parse(String name) {
        return FieldType.valueOf(name);
    }

    public static boolean isRelevant(FieldType fieldType) {
        if (COMPLETION == fieldType || MULTI_FIELD == fieldType) {
            return false;
        }

        if (IP == fieldType || GEO_POINT == fieldType || GEO_SHAPE == fieldType || NESTED == fieldType) {
            throw new UnsupportedOperationException("field " + fieldType + " not supported yet");
        }

        return true;
    }
}