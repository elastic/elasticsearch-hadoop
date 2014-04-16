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
package org.elasticsearch.hadoop.serialization.field;

import org.junit.Test;

import static org.junit.Assert.*;

import static org.hamcrest.Matchers.*;

public class DateIndexFormatterTest {

    private IndexFormatter formatter = new DateIndexFormatter();

    @Test
    public void testTimeYMDFormat() {
        formatter.configure("YYYY.MM.dd");
        assertThat(formatter.format("2014-10-06T19:20:25.000Z"), is("2014.10.06"));
    }

    @Test
    public void testTimeYMFormat() {
        formatter.configure("YYYY-MM");
        assertThat(formatter.format("2014-10-06T19:20:25.000Z"), is("2014-10"));
    }

    @Test
    public void testDateAndTimezone() {
        formatter.configure("MM-dd");
        assertThat(formatter.format("1969-08-20"), is("08-20"));
    }
}
