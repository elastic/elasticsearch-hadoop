package org.elasticsearch.hadoop.rest;

import org.elasticsearch.hadoop.util.BytesArray;
import org.elasticsearch.hadoop.util.StringUtils;

/**
 * @author vvakar
 *         Date: 8/12/13
 */
public class IndexRestClientBuffer implements RestClientBuffer {
    private byte[] buffer;
    private int bufferSize = 0;
    private int bufferEntries = 0;

    private static final byte[] INDEX_DIRECTIVE = "{\"index\":{}}\n".getBytes(StringUtils.UTF_8);
    private static final byte[] CARRIER_RETURN = "\n".getBytes(StringUtils.UTF_8);


    public IndexRestClientBuffer(final int size) {
        buffer = new byte[size];
    }

    public void write(final BytesArray scratchPad) {
        copyIntoBuffer(INDEX_DIRECTIVE, INDEX_DIRECTIVE.length);
        copyIntoBuffer(scratchPad.bytes(), scratchPad.size());
        copyIntoBuffer(CARRIER_RETURN, CARRIER_RETURN.length);
        bufferEntries++;
    }

    public byte[] getBuffer() {
        return buffer;
    }

    public void reset() {
        bufferSize = 0;
        bufferEntries = 0;
    }

    public boolean hasCapacityFor(final BytesArray scratchPad) {
        int entrySize = INDEX_DIRECTIVE.length + CARRIER_RETURN.length + scratchPad.size();
        return entrySize + bufferSize <= buffer.length;
    }

    public int numEntries() {
        return bufferEntries;
    }

    public int size() {
        return bufferSize;
    }

    private void copyIntoBuffer(byte[] data, int size) {
        System.arraycopy(data, 0, buffer, bufferSize, size);
        bufferSize += size;
    }
}
