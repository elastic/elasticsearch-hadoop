package org.elasticsearch.hadoop.serialization;

public enum FieldType {
    // Core Types
    NULL,
    STRING,
    INTEGER,
    LONG,
    FLOAT,
    DOUBLE,
    BOOLEAN,
    DATE,
    BINARY,
    // not supported yet
    IP,

    // compound types
    OBJECT;

    public static FieldType parse(String name) {
        return FieldType.valueOf(name);
    }
}