package org.elasticsearch.spark.serialization.testbeans;

import java.io.Serializable;
import java.util.Map;

/**
 * A collection of contacts
 */
public class ContactBook implements Serializable {

    private String owner;
    private Map<String, Contact> contacts;

    public ContactBook() {

    }

    public ContactBook(String owner, Map<String, Contact> contacts) {
        this.owner = owner;
        this.contacts = contacts;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public Map<String, Contact> getContacts() {
        return contacts;
    }

    public void setContacts(Map<String, Contact> contacts) {
        this.contacts = contacts;
    }
}
