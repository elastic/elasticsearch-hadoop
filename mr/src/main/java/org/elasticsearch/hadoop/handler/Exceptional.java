package org.elasticsearch.hadoop.handler;

import java.util.List;

/**
 * Any object that carries with it an internal Exception that details some kind of faulty operation or data.
 */
public interface Exceptional {

    /**
     * @return the internal exception information.
     */
    Exception getException();

    /**
     * @return any logged messages from handlers earlier in the error handler chain for why they chose to pass on
     * handling the error.
     */
    List<String> previousHandlerMessages();
}
