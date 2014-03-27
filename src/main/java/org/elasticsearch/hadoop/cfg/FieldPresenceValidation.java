package org.elasticsearch.hadoop.cfg;

public enum FieldPresenceValidation {
    IGNORE,
    WARN,
    STRICT;

    public boolean isRequired() {
        return !(IGNORE == this);
    }
}