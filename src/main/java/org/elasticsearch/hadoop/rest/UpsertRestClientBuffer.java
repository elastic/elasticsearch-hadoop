package org.elasticsearch.hadoop.rest;

import org.elasticsearch.hadoop.serialization.Parser;
import org.elasticsearch.hadoop.serialization.ParsingUtils;
import org.elasticsearch.hadoop.serialization.json.JacksonJsonParser;
import org.elasticsearch.hadoop.util.BytesArray;
import org.elasticsearch.hadoop.util.StringUtils;

/**
 * @author vvakar Date: 8/12/13
 */
public class UpsertRestClientBuffer implements RestClientBuffer {
    private byte[] buffer;
    private int bufferSize = 0;
    private int bufferEntries = 0;
    private String id_path = "url_id";

    private static final String UPSERT_DIRECTIVE_PREFIX_STR = "{\"update\":{\"_id\":\"%s\"}}\n{\"doc\":";
    private static final byte[] UPSERT_DIRECTIVE_SUFFIX = ",\"doc_as_upsert\":true}\n".getBytes(StringUtils.UTF_8);

    public UpsertRestClientBuffer(final int size, final String path) {
        buffer = new byte[size];
        this.id_path = path;
    }

    public void write(final BytesArray scratchPad) {
        byte[] updatedDirective = getDirective(scratchPad, id_path);
        copyIntoBuffer(updatedDirective, updatedDirective.length);
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
        int entrySize = getDirective(scratchPad, id_path).length + CARRIER_RETURN.length + scratchPad.size()
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

    private static String getValue(byte[] data, String path ) {
        Parser parser = new JacksonJsonParser(data);
        ParsingUtils.seek(path, parser);
        return parser.text();
    }

    private static byte[] getDirective(BytesArray scratchPad, String path) {
        final String id = getValue(scratchPad.bytes(), path);
        return String.format(UPSERT_DIRECTIVE_PREFIX_STR, id).getBytes(StringUtils.UTF_8);
    }
}
