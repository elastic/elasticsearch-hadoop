package org.elasticsearch.hadoop.mr;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.elasticsearch.hadoop.util.BytesArray;
import org.junit.Test;

import java.security.SecureRandom;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class WritableBytesConverterTests {

    @Test
    public void testConvertBytesWritable() throws Exception {
        WritableBytesConverter converter = new WritableBytesConverter();
        byte[] randomBytes = new byte[20];
        SecureRandom.getInstanceStrong().nextBytes(randomBytes);
        Object input = new BytesWritable(randomBytes);
        BytesArray output = new BytesArray(10);
        converter.convert(input, output);
        assertEquals(randomBytes.length, output.length());
        assertArrayEquals(randomBytes, output.bytes());
    }

    @Test
    public void testConvertInteger() {
        WritableBytesConverter converter = new WritableBytesConverter();
        int inputInteger = ThreadLocalRandom.current().nextInt();
        Object input = inputInteger;
        BytesArray output = new BytesArray(10);
        converter.convert(input, output);
        // Integer is not directly supported, so we expect its string value to be used:
        assertEquals(Integer.toString(inputInteger), output.toString());
    }

    @Test
    public void testConvertText() {
        WritableBytesConverter converter = new WritableBytesConverter();
        Text input = new Text("This is some text");
        BytesArray output = new BytesArray(10);
        converter.convert(input, output);
        assertEquals(input.getLength(), output.length());
        assertArrayEquals(input.getBytes(), output.bytes());
    }

    @Test
    public void testConvertByteArray() throws Exception {
        WritableBytesConverter converter = new WritableBytesConverter();
        byte[] input = new byte[20];
        SecureRandom.getInstanceStrong().nextBytes(input);
        BytesArray output = new BytesArray(10);
        converter.convert(input, output);
        assertEquals(input.length, output.length());
        assertArrayEquals(input, output.bytes());
    }

    @Test
    public void testConvertBytesArray() throws Exception {
        WritableBytesConverter converter = new WritableBytesConverter();
        byte[] randomBytes = new byte[20];
        SecureRandom.getInstanceStrong().nextBytes(randomBytes);
        Object input = new BytesArray(randomBytes);
        BytesArray output = new BytesArray(10);
        converter.convert(input, output);
        assertEquals(randomBytes.length, output.length());
        byte[] usedOutputBytes = new byte[output.length()];
        // BytesArray::bytes can give you bytes beyond length
        System.arraycopy(output.bytes(), 0, usedOutputBytes, 0, output.length());
        assertArrayEquals(randomBytes, usedOutputBytes);
    }
}
