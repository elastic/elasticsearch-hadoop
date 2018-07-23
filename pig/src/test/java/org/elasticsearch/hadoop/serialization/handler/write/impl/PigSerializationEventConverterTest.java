/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.hadoop.serialization.handler.write.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.pig.ResourceSchema;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.util.Utils;
import org.elasticsearch.hadoop.pig.PigTuple;
import org.elasticsearch.hadoop.serialization.handler.write.SerializationFailure;
import org.elasticsearch.hadoop.util.DateUtils;
import org.elasticsearch.hadoop.util.StringUtils;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class PigSerializationEventConverterTest {

    @Test
    public void generateEventPigTuple() throws Exception {
        Map<String, Number> map = new LinkedHashMap<String, Number>();
        map.put("one", 1);
        map.put("two", 2);
        map.put("three", 3);

        PigTuple tuple = createTuple(TupleFactory.getInstance().newTuple(Arrays.asList(new String[] { "one", "two" })),
                createSchema("namedtuple: (first:chararray, second:chararray)"));

        SerializationEventConverter eventConverter = new SerializationEventConverter();

        SerializationFailure iaeFailure = new SerializationFailure(new IllegalArgumentException("garbage"), tuple, new ArrayList<String>());

        String rawEvent = eventConverter.getRawEvent(iaeFailure);
        assertThat(rawEvent, equalTo("[PigTuple]=((namedtuple:(first:chararray,second:chararray)):((one,two)))"));
        String timestamp = eventConverter.getTimestamp(iaeFailure);
        assertTrue(StringUtils.hasText(timestamp));
        assertTrue(DateUtils.parseDate(timestamp).getTime().getTime() > 1L);
        String exceptionType = eventConverter.renderExceptionType(iaeFailure);
        assertEquals("illegal_argument_exception", exceptionType);
        String exceptionMessage = eventConverter.renderExceptionMessage(iaeFailure);
        assertEquals("garbage", exceptionMessage);
        String eventMessage = eventConverter.renderEventMessage(iaeFailure);
        assertEquals("Could not construct bulk entry from record", eventMessage);
    }

    private ResourceSchema createSchema(String schema) {
        try {
            return new ResourceSchema(Utils.getSchemaFromString(schema));
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private PigTuple createTuple(Object obj, ResourceSchema schema) {
        PigTuple tuple = new PigTuple(schema);
        tuple.setTuple(TupleFactory.getInstance().newTuple(obj));
        return tuple;
    }
}
