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

package org.elasticsearch.hadoop.serialization.builder;

import java.util.ArrayList;
import java.util.Collection;

import org.elasticsearch.hadoop.serialization.FieldType;
import org.elasticsearch.hadoop.serialization.json.JacksonJsonParser;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static org.elasticsearch.hadoop.serialization.builder.AbstractExtendedBooleanValueReaderTest.ExpectedOutcome.*;

/**
 * Test meant to exercise the extended boolean parsing logic from Elasticsearch
 */
@RunWith(Parameterized.class)
public abstract class AbstractExtendedBooleanValueReaderTest {

    /**
     * This is because nulls are treated oddly across scala and java language boundaries.
     */
    public enum ExpectedOutcome {
        NULL, TRUE, FALSE
    }

    protected ValueReader vr;

    public abstract ValueReader createValueReader();
    public abstract Matcher<Object> isTrue();
    public abstract Matcher<Object> isFalse();
    public abstract Matcher<Object> isNull();

    @Parameters
    public static Collection<Object[]> params() {
        Collection<Object[]> params = new ArrayList<Object[]>();

        // JSON null value converts to null, while empty string ("") converts to false
        params.add(new Object[]{"null", NULL});
        params.add(new Object[]{"\"\"", FALSE});

        // Booleans convert as expected
        params.add(new Object[]{"false", FALSE});
        params.add(new Object[]{"true",  TRUE});

        // String values are checked against the values "false", "off", "no", "0", and empty string ("")
        // All other String values are considered true by virtue of them not being equal to the previous Strings.

        // Strings named after booleans convert as expected.
        params.add(new Object[]{"\"false\"", FALSE});
        params.add(new Object[]{"\"true\"",  TRUE});

        // Alias of "on/off" for booleans
        params.add(new Object[]{"\"on\"",  TRUE});
        params.add(new Object[]{"\"off\"", FALSE});

        // Alias of "yes/no" for booleans
        params.add(new Object[]{"\"yes\"", TRUE});
        params.add(new Object[]{"\"no\"",  FALSE});

        // Integral number types (int, long) are false if (n == 0) otherwise they are true.
        params.add(new Object[]{"1",  TRUE});
        params.add(new Object[]{"0",  FALSE});
        params.add(new Object[]{"-1", TRUE});

        // Integrals inside strings are not special, therefor they follow string rules.
        params.add(new Object[]{"\"1\"",  TRUE});
        params.add(new Object[]{"\"-1\"", TRUE});

        // Except for the literal string "0", which IS special and is directly checked in the String logic.
        params.add(new Object[]{"\"0\"", FALSE});

        // Floating point numbers (float, double) are casted to integrals by truncating decimal places.
        // They are handled like integral values from there.
        params.add(new Object[]{"0.0", FALSE});
        params.add(new Object[]{"0.1", FALSE});

        // Floating point numbers inside strings are not special, therefor they follow string rules.
        params.add(new Object[]{"\"0.0\"", TRUE});
        params.add(new Object[]{"\"0.1\"", TRUE});

        return params;
    }

    private final String jsonInput;
    private final ExpectedOutcome expected;

    public AbstractExtendedBooleanValueReaderTest(String jsonInput, ExpectedOutcome expected) {
        this.jsonInput = jsonInput;
        this.expected = expected;
    }

    @Before
    public void start() {
        vr = createValueReader();
    }

    @Test
    public void testConvertBoolean() throws Exception {
        verify(jsonInput, expected);
    }

    private void verify(String jsonValue, ExpectedOutcome expected) {
        JacksonJsonParser parser = new JacksonJsonParser(jsonValue.getBytes());
        parser.nextToken();
        Object out = vr.readValue(parser, parser.text(), FieldType.BOOLEAN);

        if (expected == NULL) {
            Assert.assertThat(String.format("Input value was [%s]", jsonValue), out, isNull());
        } else {
            Assert.assertThat(String.format("Input value was [%s]", jsonValue), out, Matchers.not(isNull()));
            if (expected == TRUE) {
                Assert.assertThat(String.format("Input value was [%s]", jsonValue), out, isTrue());
            } else {
                Assert.assertThat(String.format("Input value was [%s]", jsonValue), out, isFalse());
            }
        }
    }

}