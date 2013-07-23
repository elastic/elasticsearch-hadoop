package org.elasticsearch.hadoop.serialization;

import org.elasticsearch.hadoop.util.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ValueReaderTest {

    private byte[] content;

    @Before
    public void before() throws Exception {
        content = TestUtils.fromInputStream(getClass().getResourceAsStream("scroll-test.json"));
    }

    @After
    public void after() {
        content = null;
    }

    @Test
    public void testSimplePathReader() throws Exception {
        ScrollReader reader = new ScrollReader(new SimpleValueReader(), null);
        System.out.println(reader.read(content));
    }
}
