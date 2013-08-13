package org.elasticsearch.hadoop.rest;

import org.elasticsearch.hadoop.util.BytesArray;
import org.elasticsearch.hadoop.util.StringUtils;

/**
 * @author vvakar Date: 8/12/13
 */
public class UpsertRestClientBuffer implements RestClientBuffer {
    private byte[] buffer;
    private int bufferSize = 0;
    private int bufferEntries = 0;

    private static final byte[] UPSERT_DIRECTIVE_PREFIX = "{\"update\":{}}\n{\"doc_as_upsert\":true, \"doc\":"
            .getBytes(StringUtils.UTF_8);
    private static final byte[] UPSERT_DIRECTIVE_SUFFIX = "}".getBytes(StringUtils.UTF_8);

    public UpsertRestClientBuffer(final int size) {
        buffer = new byte[size];
    }

    public void write(final BytesArray scratchPad) {
        copyIntoBuffer(UPSERT_DIRECTIVE_PREFIX, UPSERT_DIRECTIVE_PREFIX.length);
        copyIntoBuffer(scratchPad.bytes(), scratchPad.size());
        copyIntoBuffer(UPSERT_DIRECTIVE_SUFFIX, UPSERT_DIRECTIVE_SUFFIX.length);
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
        int entrySize = UPSERT_DIRECTIVE_PREFIX.length + CARRIER_RETURN.length + scratchPad.size()
                + UPSERT_DIRECTIVE_SUFFIX.length;
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
