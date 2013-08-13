package org.elasticsearch.hadoop.rest;

import org.elasticsearch.hadoop.util.BytesArray;
import org.elasticsearch.hadoop.util.StringUtils;

/**
 * @author vvakar
 *         Date: 8/12/13
 */
public interface RestClientBuffer {
    static final byte[] CARRIER_RETURN = "\n".getBytes(StringUtils.UTF_8);

    void write(final BytesArray scratchPad);
    byte[] getBuffer();
    void reset();
    boolean hasCapacityFor(final BytesArray scratchPad);
    int numEntries();
    int size();
}
