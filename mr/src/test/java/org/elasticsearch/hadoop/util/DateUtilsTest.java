package org.elasticsearch.hadoop.util;

import org.junit.Test;

import java.sql.Timestamp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class DateUtilsTest {
    @Test
    public void parseDateNanos() {
        Timestamp timestamp = DateUtils.parseDateNanos("2015-01-01");
        assertNotNull(timestamp);
        assertEquals(1420070400000l, timestamp.getTime());
        assertEquals(0, timestamp.getNanos());

        timestamp = DateUtils.parseDateNanos("2015-01-01T12:10:30.123456789Z");
        assertNotNull(timestamp);
        assertEquals(1420114230123l, timestamp.getTime());
        assertEquals(123456789, timestamp.getNanos());

        timestamp = DateUtils.parseDateNanos("2015-01-01T00:00:00.000Z");
        assertNotNull(timestamp);
        assertEquals(1420070400000l, timestamp.getTime());
        assertEquals(0, timestamp.getNanos());

        timestamp = DateUtils.parseDateNanos("2015-01-01T12:10:30.123456Z");
        assertNotNull(timestamp);
        assertEquals(1420114230123l, timestamp.getTime());
        assertEquals(123456000, timestamp.getNanos());

        timestamp = DateUtils.parseDateNanos("2015-01-01T12:10:30.123Z");
        assertNotNull(timestamp);
        assertEquals(1420114230123l, timestamp.getTime());
        assertEquals(123000000, timestamp.getNanos());
    }
}
