package org.elasticsearch.hadoop.util;

import org.junit.Test;

import static org.junit.Assert.*;

public class BytesArrayPoolTest {

    @Test
    public void testAddAndReset() throws Exception {
        BytesArrayPool pool = new BytesArrayPool();

        pool.get().bytes("Test");
        pool.get().bytes("Data");
        pool.get().bytes("Rules");

        BytesRef ref = new BytesRef();

        assertEquals(13, pool.length());
        ref.add(pool);
        assertEquals("TestDataRules", ref.toString());

        BytesRef ref2 = new BytesRef();

        pool.reset();

        pool.get().bytes("New");
        pool.get().bytes("Info");

        assertEquals(7, pool.length());
        ref2.add(pool);
        assertEquals("NewInfo", ref2.toString());
    }
}