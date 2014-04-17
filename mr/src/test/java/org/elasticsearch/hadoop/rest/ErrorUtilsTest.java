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
package org.elasticsearch.hadoop.rest;

import org.junit.Test;

import static org.junit.Assert.*;

import static org.hamcrest.CoreMatchers.*;

public class ErrorUtilsTest {

    @Test
    public void testname() throws Exception {
        String error = "MapperParsingException[failed to parse]; nested: ElasticSearchParseException[Failed to derive xcontent from (offset=0, length=14): "
                + "[83, 116, 114, 101, 97, 109, 32, 99, 108, 111, 115, 101, 100, 46]]";
        assertThat(ErrorUtils.extractInvalidXContent(error), is("Stream closed."));
    }
}
