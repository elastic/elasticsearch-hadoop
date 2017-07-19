package org.elasticsearch.spark.serialization.testbeans;

import java.io.Serializable;

/**
 * Bean object with contact info for tests.
 */
public class Contact implements Serializable {

    private String name;
    private String relation;

    public Contact() {
    }

    public Contact(String name, String relation) {
        this.name = name;
        this.relation = relation;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getRelation() {
        return relation;
    }

    public void setRelation(String relation) {
        this.relation = relation;
    }
}
