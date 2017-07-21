package org.elasticsearch.hadoop.serialization.json;

import java.io.ByteArrayOutputStream;

import org.elasticsearch.hadoop.serialization.Generator;
import org.junit.Test;

import static org.junit.Assert.*;

public class JacksonJsonGeneratorTest {

    @Test
    public void getParentPath() throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(256);
        Generator generator = new JacksonJsonGenerator(bos);

        // root level, no parents here
        generator.writeBeginObject();
        assertEquals("", generator.getParentPath());

        // We're still in the root level. "test" is just current field.
        generator.writeFieldName("test");
        assertEquals("", generator.getParentPath());

        // Nest into an object. "test" is the parent to all following fields.
        generator.writeBeginObject();
        assertEquals("test", generator.getParentPath());

        // "test" still parent. "subfield" is just current field
        generator.writeFieldName("subfield");
        assertEquals("test", generator.getParentPath());

        // Nest into another object. "test.subfield" is now parent to following fields.
        generator.writeBeginObject();
        assertEquals("test.subfield", generator.getParentPath());

        // Still same parent, "subsubfield" is just current field
        generator.writeFieldName("subsubfield");
        assertEquals("test.subfield", generator.getParentPath());

        // Still same parent
        generator.writeString("value");
        assertEquals("test.subfield", generator.getParentPath());

        // End second nesting, so we're done with the "subfield" object. "test" is back to being parent
        generator.writeEndObject();
        assertEquals("test", generator.getParentPath());

        // End first nesting. Back at root. No parents.
        generator.writeEndObject();
        assertEquals("", generator.getParentPath());

        // End root object. No parents
        generator.writeEndObject();
        assertEquals("", generator.getParentPath());

        generator.flush();
        assertEquals("{\"test\":{\"subfield\":{\"subsubfield\":\"value\"}}}", new String(bos.toByteArray()));
    }
}
