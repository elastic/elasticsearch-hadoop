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

import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;

import static org.junit.Assert.*;

public class FieldFilterTest {

    @Test
    public void testNoIncludes() {
        assertTrue(filter("foo.bar", null, null));
    }

    @Test
    public void testOnlyIncludesNotMatching() {
        assertFalse(filter("foo.bar", Arrays.asList("bar"), null));
    }

    @Test
    public void testOnlyIncludesExactMatch() {
        assertTrue(filter("foo.bar", Arrays.asList("foo.bar"), null));
    }

    @Test
    public void testOnlyIncludesTopLevelMatchWithoutPattern() {
        assertFalse(filter("foo.bar", Arrays.asList("foo"), null));
    }

    @Test
    public void testOnlyIncludesTopLevelMatchWithPattern() {
        assertTrue(filter("foo.bar", Arrays.asList( "foo.*"), null));
    }

    @Test
    public void testOnlyIncludesNestedMatch() {
        assertTrue(filter("foo.bar", Arrays.asList( "*.bar"), null));
    }

    @Test
    public void testOnlyIncludesNestedPattern() {
        assertTrue(filter("foo.bar.taz", Arrays.asList( "foo.*ar.taz"), null));
    }

    @Test
    public void testOnlyIncludesNestedPatternNotMatching() {
        assertFalse(filter("foo.bar.taz", Arrays.asList( "foo.br*.taz"), null));
    }

    @Test
    public void testOnlyExcludesPartialMatch() {
        assertTrue(filter("foo.bar", null, Arrays.asList("foo")));
    }

    @Test
    public void testOnlyExcludesWithExactMatch() {
        assertFalse(filter("foo.bar", null, Arrays.asList("foo.bar")));
    }

    @Test
    public void testOnlyExcludesWithTopPatternMatch() {
        assertFalse(filter("foo.bar", null, Arrays.asList("foo*")));
    }

    @Test
    public void testOnlyExcludesWithNestedPatternMatch() {
        assertFalse(filter("foo.bar", null, Arrays.asList("*.bar")));
    }

    @Test
    public void testOnlyExcludesWithNestedMiddlePatternMatch() {
        assertFalse(filter("foo.bar.taz", null, Arrays.asList("foo.*.taz")));
    }

    @Test
    public void testIncludeAndExcludeExactMatch() {
        assertFalse(filter("foo.bar", Arrays.asList("foo", "foo.bar"), Arrays.asList("foo.bar")));
    }

    @Test
    public void testIncludeTopMatchWithExcludeNestedExactMatch() {
        assertFalse(filter("foo.bar.taz", Arrays.asList("foo.bar.*"), Arrays.asList("foo.*.taz")));
    }

    @Test
    public void testIncludeExactMatchWithExcludePattern() {
        assertFalse(filter("foo.bar", Arrays.asList("foo.bar"), Arrays.asList("foo.*")));
    }

    @Test
    public void testMatchNonExisting() {
        assertFalse(filter("nested.what", Arrays.asList("nested.bar"), null));
    }

    public static boolean filter(String path, Collection<String> includes, Collection<String> excludes) {
        return FieldFilter.filter(path, FieldFilter.toNumberedFilter(includes), excludes, true).matched;
    }
}