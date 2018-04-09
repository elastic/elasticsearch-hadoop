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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.hamcrest.Matchers;
import org.junit.Test;

import static org.junit.Assert.*;

public class FieldFilterTest {

    @Test
    public void testFilterNoIncludes() {
        assertTrue(filter("foo.bar", null, null));
    }

    @Test
    public void testFilterOnlyIncludesNotMatching() {
        assertFalse(filter("foo.bar", Arrays.asList("bar"), null));
    }

    @Test
    public void testFilterOnlyIncludesPartialMatching() {
        assertFalse(filter("fo", Arrays.asList("foo.bar.baz"), null));
        assertTrue(filter("foo", Arrays.asList("foo.bar.baz"), null));
        assertFalse(filter("foo.ba", Arrays.asList("foo.bar.baz"), null));
        assertTrue(filter("foo.bar", Arrays.asList("foo.bar.baz"), null));
        assertFalse(filter("foo.bar.ba", Arrays.asList("foo.bar.baz"), null));
        assertTrue(filter("foo.bar.baz", Arrays.asList("foo.bar.baz"), null));
        assertFalse(filter("foo.bar.baz.qux", Arrays.asList("foo.bar.baz"), null));
    }

    @Test
    public void testFilterOnlyIncludesExactMatch() {
        assertTrue(filter("foo.bar", Arrays.asList("foo.bar"), null));
    }

    @Test
    public void testFilterOnlyIncludesTopLevelMatchWithoutPattern() {
        assertFalse(filter("foo.bar", Arrays.asList("foo"), null));
    }

    @Test
    public void testFilterOnlyIncludesTopLevelMatchWithPattern() {
        assertTrue(filter("foo.bar", Arrays.asList( "foo.*"), null));
    }

    @Test
    public void testFilterOnlyIncludesNestedMatch() {
        assertTrue(filter("foo.bar", Arrays.asList( "*.bar"), null));
    }

    @Test
    public void testFilterOnlyIncludesNestedPattern() {
        assertTrue(filter("foo.bar.taz", Arrays.asList( "foo.*ar.taz"), null));
    }

    @Test
    public void testFilterOnlyIncludesNestedPatternNotMatching() {
        assertFalse(filter("foo.bar.taz", Arrays.asList( "foo.br*.taz"), null));
    }

    @Test
    public void testFilterOnlyExcludesPartialMatch() {
        assertTrue(filter("foo.bar", null, Arrays.asList("foo")));
    }

    @Test
    public void testFilterOnlyExcludesWithExactMatch() {
        assertFalse(filter("foo.bar", null, Arrays.asList("foo.bar")));
    }

    @Test
    public void testFilterOnlyExcludesWithTopPatternMatch() {
        assertFalse(filter("foo.bar", null, Arrays.asList("foo*")));
    }

    @Test
    public void testFilterOnlyExcludesWithNestedPatternMatch() {
        assertFalse(filter("foo.bar", null, Arrays.asList("*.bar")));
    }

    @Test
    public void testFilterOnlyExcludesWithNestedMiddlePatternMatch() {
        assertFalse(filter("foo.bar.taz", null, Arrays.asList("foo.*.taz")));
    }

    @Test
    public void testFilterIncludeAndExcludeExactMatch() {
        assertFalse(filter("foo.bar", Arrays.asList("foo", "foo.bar"), Arrays.asList("foo.bar")));
    }

    @Test
    public void testFilterIncludeTopMatchWithExcludeNestedExactMatch() {
        assertFalse(filter("foo.bar.taz", Arrays.asList("foo.bar.*"), Arrays.asList("foo.*.taz")));
    }

    @Test
    public void testFilterIncludeExactMatchWithExcludePattern() {
        assertFalse(filter("foo.bar", Arrays.asList("foo.bar"), Arrays.asList("foo.*")));
    }

    @Test
    public void testFilterMatchNonExisting() {
        assertFalse(filter("nested.what", Arrays.asList("nested.bar"), null));
    }

    public static boolean filter(String path, Collection<String> includes, Collection<String> excludes) {
        return FieldFilter.filter(path, FieldFilter.toNumberedFilter(includes), excludes, true).matched;
    }

    @Test(expected = EsHadoopIllegalArgumentException.class)
    public void testCreateMalformedFilter() {
        FieldFilter.toNumberedFilter(Arrays.asList("a:broken"));
    }

    @Test
    public void testCreateSimpleFilter() {
        assertThat(
                new ArrayList<FieldFilter.NumberedInclude>(FieldFilter.toNumberedFilter(Arrays.asList("a"))),
                Matchers.contains(new FieldFilter.NumberedInclude("a", 1))
        );
    }

    @Test
    public void testCreateMultipleSimpleFilters() {
        assertThat(
                new ArrayList<FieldFilter.NumberedInclude>(FieldFilter.toNumberedFilter(Arrays.asList("a", "b"))),
                Matchers.contains(new FieldFilter.NumberedInclude("a", 1), new FieldFilter.NumberedInclude("b", 1))
        );
    }

    @Test
    public void testCreateSimpleNumberedFilter() {
        assertThat(
                new ArrayList<FieldFilter.NumberedInclude>(FieldFilter.toNumberedFilter(Arrays.asList("a:2"))),
                Matchers.contains(new FieldFilter.NumberedInclude("a", 2))
        );
    }

    @Test
    public void testCreateMultipleNumberedFilters() {
        assertThat(
                new ArrayList<FieldFilter.NumberedInclude>(FieldFilter.toNumberedFilter(Arrays.asList("a:2", "b:4"))),
                Matchers.contains(new FieldFilter.NumberedInclude("a", 2), new FieldFilter.NumberedInclude("b", 4))
        );
    }

    @Test
    public void testCreateMultipleMixedFilters() {
        assertThat(
                new ArrayList<FieldFilter.NumberedInclude>(FieldFilter.toNumberedFilter(Arrays.asList("a:2", "b:4", "c"))),
                Matchers.contains(new FieldFilter.NumberedInclude("a", 2), new FieldFilter.NumberedInclude("b", 4), new FieldFilter.NumberedInclude("c", 1))
        );
    }
}