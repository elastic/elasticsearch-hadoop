package org.elasticsearch.hadoop.handler;

/**
 * Any object that carries with it an internal Exception that details some kind of faulty operation or data.
 */
public interface Exceptional {

    /**
     * @return the internal exception information.
     */
    Exception getException();
}
