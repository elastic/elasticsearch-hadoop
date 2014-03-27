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
package org.elasticsearch.hadoop.util;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import static org.junit.Assert.*;

import static org.elasticsearch.hadoop.util.StringUtils.*;

import static org.hamcrest.Matchers.*;

public class LevenshteinTests {

    Map<String, String> props;
    Set<String> orig = new LinkedHashSet<String>();

    public LevenshteinTests() {
        orig.add("foo");
        orig.add("foo.bar123");
        orig.add("foo.bar123.abcdefghijklmn");
        orig.add("foo.bar123.abcdefghijklmn.xyz890");

        props = unroll(orig);
    }

    private Map<String, String> unroll(Set<String> props) {
        Map<String, String> col = new LinkedHashMap<String, String>();

        for (String string : props) {
            int match = string.lastIndexOf(".");
            col.put((match > 0 ? string.substring(match + 1) : string), string);
        }

        return col;
    }

    @Test
    public void testDistance() {
        assertThat(levenshteinDistance("bar", "bor"), is(1));
        assertThat(levenshteinDistance("bar", "bara"), is(1));
        assertThat(levenshteinDistance("bar", "abar"), is(1));
        assertThat(levenshteinDistance("bar", "abara"), is(2));
        assertThat(levenshteinDistance("bar", "abora"), is(3));
        assertThat(levenshteinDistance("bar", "arb"), is(2));
        assertThat(levenshteinDistance("bar", "aarb"), is(2));
        assertThat(levenshteinDistance("bar", "aarbx"), is(3));
    }

    @Test
    public void testFindTypos() {
        Set<String> keySet = props.keySet();

        assertThat(findSimiliar("foo", keySet), contains("foo"));
        assertThat(findSimiliar("ofo", keySet), contains("foo"));
        assertThat(findSimiliar("oof", keySet), contains("foo"));

        assertThat(findSimiliar("ar123", keySet), contains("bar123"));
        assertThat(findSimiliar("ba123", keySet), contains("bar123"));
        assertThat(findSimiliar("abr123", keySet), contains("bar123"));

        assertThat(findSimiliar("xyz890", keySet), contains("xyz890"));
        assertThat(findSimiliar("xyz89", keySet), contains("xyz890"));
        assertThat(findSimiliar("yzx890", keySet), contains("xyz890"));
        assertThat(findSimiliar("yz890", keySet), contains("xyz890"));
    }
}