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

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

public class JdkExtendedBooleanValueReaderReaderTest extends AbstractExtendedBooleanValueReaderTest {

    public JdkExtendedBooleanValueReaderReaderTest(String jsonInput, ExpectedOutcome expected) {
        super(jsonInput, expected);
    }

    @Override
    public ValueReader createValueReader() {
        return new JdkValueReader();
    }

    @Override
    public Matcher<Object> isTrue() {
        return new BaseMatcher<Object>() {
            @Override
            public boolean matches(Object item) {
                return item.equals(Boolean.TRUE);
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("is true");
            }
        };
    }

    @Override
    public Matcher<Object> isFalse() {
        return new BaseMatcher<Object>() {
            @Override
            public boolean matches(Object item) {
                return item.equals(Boolean.FALSE);
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("is false");
            }
        };
    }

    @Override
    public Matcher<Object> isNull() {
        return new BaseMatcher<Object>() {
            @Override
            public boolean matches(Object item) {
                return item == null;
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("is null");
            }
        };
    }
}
