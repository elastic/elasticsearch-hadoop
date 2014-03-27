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

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;


/**
 * Utility class around Strings. Used to remove dependency on other libraries that might (or not) be available at runtime.
 */
public abstract class StringUtils {

    public static final Charset UTF_8 = Charset.forName("UTF-8");
    public static final String EMPTY = "";

    public static boolean hasLength(CharSequence sequence) {
        return (sequence != null && sequence.length() > 0);
    }

    public static boolean hasText(CharSequence sequence) {
        if (!hasLength(sequence)) {
            return false;
        }
        int length = sequence.length();
        for (int i = 0; i < length; i++) {
            if (!Character.isWhitespace(sequence.charAt(i))) {
                return true;
            }
        }
        return false;
    }

    public static List<String> tokenize(String string) {
        return tokenize(string, ",");
    }

    public static List<String> tokenize(String string, String delimiters) {
        return tokenize(string, delimiters, true, true);
    }

    public static List<String> tokenize(String string, String delimiters, boolean trimTokens, boolean ignoreEmptyTokens) {
        if (string == null) {
            return Collections.emptyList();
        }
        StringTokenizer st = new StringTokenizer(string, delimiters);
        List<String> tokens = new ArrayList<String>();
        while (st.hasMoreTokens()) {
            String token = st.nextToken();
            if (trimTokens) {
                token = token.trim();
            }
            if (!ignoreEmptyTokens || token.length() > 0) {
                tokens.add(token);
            }
        }
        return tokens;
    }

    public static String concatenate(Collection<?> list, String delimiter) {
        if (list == null || list.isEmpty()) {
            return EMPTY;
        }
        if (delimiter == null) {
            delimiter = EMPTY;
        }
        StringBuilder sb = new StringBuilder();

        for (Object object : list) {
            sb.append(object.toString());
            sb.append(delimiter);
        }

        sb.setLength(sb.length() - delimiter.length());
        return sb.toString();
    }

    public static String concatenate(Object[] array, String delimiter) {
        if (array == null || array.length == 0) {
            return EMPTY;
        }
        if (delimiter == null) {
            delimiter = EMPTY;
        }

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < array.length; i++) {
            if (i > 0) {
                sb.append(delimiter);
            }
            sb.append(array[i]);
        }
        return sb.toString();
    }

    public static String deleteWhitespace(CharSequence sequence) {
        if (!hasLength(sequence)) {
            return EMPTY;
        }

        StringBuilder sb = new StringBuilder(sequence.length());
        for (int i = 0; i < sequence.length(); i++) {
            char currentChar = sequence.charAt(i);
            if (!Character.isWhitespace(currentChar)) {
                sb.append(currentChar);
            }
        }
        // return the initial String if no whitespace is found
        return (sb.length() == sequence.length() ? sequence.toString() : sb.toString());
    }

    public static String asUTFString(byte[] content) {
        return asUTFString(content, content.length);
    }

    public static String asUTFString(byte[] content, int length) {
        return (content == null || length == 0 ? EMPTY : new String(content, 0, length, UTF_8));
    }

    public static byte[] toUTF(String string) {
        return string.getBytes(UTF_8);
    }

    public static int levenshteinDistance(CharSequence s1, CharSequence s2) {
        int[] prev = new int[s2.length() + 1];
        for (int j = 0; j < s2.length() + 1; j++) {
            prev[j] = j;
        }

        for (int i = 1; i < s1.length() + 1; i++) {
            int[] curr = new int[s2.length() + 1];
            curr[0] = i;

            for (int j = 1; j < s2.length() + 1; j++) {
                int d1 = prev[j] + 1;
                int d2 = curr[j - 1] + 1;
                int d3 = prev[j - 1];
                if (s1.charAt(i - 1) != s2.charAt(j - 1)) {
                    d3 += 1;
                }
                curr[j] = Math.min(Math.min(d1, d2), d3);
            }
            prev = curr;
        }
        return prev[s2.length()];
    }

    public static List<String> findSimiliar(CharSequence match, Collection<String> potential) {
        List<String> list = new ArrayList<String>(3);

        // 1 switches or 1 extra char
        int maxDistance = 2;

        for (String string : potential) {
            int dist = levenshteinDistance(match, string);
            if (dist < maxDistance) {
                maxDistance = dist;
                list.clear();
                list.add(string);
            }
            else if (dist == maxDistance) {
                list.add(string);
            }
        }

        return list;
    }
}