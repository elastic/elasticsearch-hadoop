package org.elasticsearch.hadoop.serialization.builder;

import org.elasticsearch.hadoop.serialization.Generator;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;

import static org.junit.Assert.assertEquals;

public class JdkValueWriterTest {
    @Test
    public void testWriteDate() {
        JdkValueWriter jdkValueWriter = new JdkValueWriter();
        Date date = new Date(1420114230123l);
        Generator generator = Mockito.mock(Generator.class);
        ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
        jdkValueWriter.doWrite(date, generator, "");
        Mockito.verify(generator).writeString(argument.capture());
        String expected = date.toInstant().atZone(ZoneId.systemDefault()).toOffsetDateTime().toString();
        String actual = argument.getValue();
        assertEquals(expected, actual);
        OffsetDateTime parsedDate = DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(actual, OffsetDateTime::from);
        assertEquals(123000000, parsedDate.getNano()); //Nothing beyond milliseconds
    }

    @Test
    public void testWriteDateWithNanos() {
        JdkValueWriter jdkValueWriter = new JdkValueWriter();
        Timestamp timestamp = new Timestamp(1420114230123l);
        timestamp.setNanos(123456789);
        Generator generator = Mockito.mock(Generator.class);
        ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
        jdkValueWriter.doWrite(timestamp, generator, "");
        Mockito.verify(generator).writeString(argument.capture());
        String expected = timestamp.toInstant().atZone(ZoneId.systemDefault()).toOffsetDateTime().toString();
        String actual = argument.getValue();
        assertEquals(expected, actual);
        OffsetDateTime parsedDate = DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(actual, OffsetDateTime::from);
        assertEquals(123456789, parsedDate.getNano());
    }
}
