package org.elasticsearch.hadoop.serialization;

import java.io.InputStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ValueReaderTest {

    private InputStream in;

    @Before
    public void before() throws Exception {
        in = getClass().getResourceAsStream("scroll-test.json");
    }

    @After
    public void after() throws Exception {
        in.close();
    }

    @Test
    public void testSimplePathReader() throws Exception {
        ScrollReader reader = new ScrollReader(new JdkValueReader(), null);
        System.out.println(reader.read(in));
    }
}
