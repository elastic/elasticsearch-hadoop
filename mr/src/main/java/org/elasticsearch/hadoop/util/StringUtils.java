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

import org.codehaus.jackson.io.JsonStringEncoder;
import org.elasticsearch.hadoop.EsHadoopIllegalStateException;
import org.elasticsearch.hadoop.serialization.json.BackportedJsonStringEncoder;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;


/**
 * Utility class around Strings. Used to remove dependency on other libraries that might (or not) be available at runtime.
 */
public abstract class StringUtils {

    public static final Charset UTF_8 = Charset.forName("UTF-8");
    public static final String EMPTY = "";
    public static final String DEFAULT_DELIMITER = ",";
    public static final String SLASH = "/";
    public static final String PATH_TOP = "..";
    public static final String PATH_CURRENT = ".";
    public static final String SOURCE_FIELD_NAME = "_source.";
    public static final String FIELD_FIELD_NAME = "fields.";
    public static final String SOURCE_ROOT = "hits.hits."+SOURCE_FIELD_NAME;
    public static final String FIELDS_ROOT = "hits.hits."+FIELD_FIELD_NAME;
    public static final String[] EMPTY_ARRAY = new String[0];

    private static final boolean HAS_JACKSON_CLASS = ObjectUtils.isClassPresent("org.codehaus.jackson.io.JsonStringEncoder", StringUtils.class.getClassLoader());

    public static class IpAndPort {
        public final String ip;
        public final int port;

        IpAndPort(String ip, int port) {
            this.ip = ip;
            this.port = port;
        }

        IpAndPort(String ip) {
            this.ip = ip;
            this.port = 0;
        }

        @Override
        public String toString() {
            return (port > 0 ? ip + ":" + port : ip);
        }
    }

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

    public static boolean hasWhitespace(CharSequence sequence) {
        if (!hasLength(sequence)) {
            return false;
        }
        int length = sequence.length();
        for (int i = 0; i < length; i++) {
            if (Character.isWhitespace(sequence.charAt(i))) {
                return true;
            }
        }
        return false;
    }

    public static int countOccurrences(String string, String substring) {
        if (string == null || substring == null || string.length() == 0 || substring.length() == 0) {
            return 0;
        }
        int count = 0;
        int currentPosition = 0;
        int index;
        while ((index = string.indexOf(substring, currentPosition)) != -1) {
            ++count;
            currentPosition = index + substring.length();
        }
        return count;
    }

    public static List<String> tokenize(String string) {
        return tokenize(string, DEFAULT_DELIMITER);
    }

    public static List<String> tokenize(String string, String delimiters) {
        return tokenize(string, delimiters, true, true);
    }

    public static List<String> tokenize(String string, String delimiters, boolean trimTokens, boolean ignoreEmptyTokens) {
        if (!StringUtils.hasText(string)) {
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

    public static String concatenate(Collection<?> list) {
        return concatenate(list, DEFAULT_DELIMITER);
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

    public static String trimWhitespace(String string) {
        if (!hasLength(string)) {
            return string;
        }
        StringBuilder sb = new StringBuilder(string);
        while (sb.length() > 0 && Character.isWhitespace(sb.charAt(0))) {
            sb.deleteCharAt(0);
        }
        while (sb.length() > 0
                && Character.isWhitespace(sb.charAt(sb.length() - 1))) {
            sb.deleteCharAt(sb.length() - 1);
        }
        // try to return the initial string if possible
        return (sb.length() == string.length() ? string : sb.toString());
    }

    public static String asUTFString(byte[] content) {
        return asUTFString(content, 0, content.length);
    }

    public static String asUTFString(byte[] content, int offset, int length) {
        return (content == null || length == 0 ? EMPTY : new String(content, offset, length, UTF_8));
    }

    public static byte[] toUTF(String string) {
        return string.getBytes(UTF_8);
    }

    // Based on "Algorithms on Strings, Trees and Sequences by Dan Gusfield".
    // returns -1 if the two strings are within the given threshold of each other, -1 otherwise
    public static int levenshteinDistance(CharSequence one, CharSequence another, int threshold) {
        int n = one.length();
        int m = another.length();

        // if one string is empty, the edit distance is necessarily the length of the other
        if (n == 0) {
            return m <= threshold ? m : -1;
        }
        else if (m == 0) {
            return n <= threshold ? n : -1;
        }

        if (n > m) {
            // swap the two strings to consume less memory
            final CharSequence tmp = one;
            one = another;
            another = tmp;
            n = m;
            m = another.length();
        }

        int p[] = new int[n + 1]; // 'previous' cost array, horizontally
        int d[] = new int[n + 1]; // cost array, horizontally
        int _d[]; // placeholder to assist in swapping p and d

        // fill in starting table values
        final int boundary = Math.min(n, threshold) + 1;
        for (int i = 0; i < boundary; i++) {
            p[i] = i;
        }

        // these fills ensure that the value above the rightmost entry of our
        // stripe will be ignored in following loop iterations
        Arrays.fill(p, boundary, p.length, Integer.MAX_VALUE);
        Arrays.fill(d, Integer.MAX_VALUE);

        for (int j = 1; j <= m; j++) {
            final char t_j = another.charAt(j - 1);
            d[0] = j;

            // compute stripe indices, constrain to array size
            final int min = Math.max(1, j - threshold);
            final int max = (j > Integer.MAX_VALUE - threshold) ? n : Math.min(n, j + threshold);

            // the stripe may lead off of the table if s and t are of different sizes
            if (min > max) {
                return -1;
            }

            // ignore entry left of leftmost
            if (min > 1) {
                d[min - 1] = Integer.MAX_VALUE;
            }

            // iterates through [min, max] in s
            for (int i = min; i <= max; i++) {
                if (one.charAt(i - 1) == t_j) {
                    // diagonally left and up
                    d[i] = p[i - 1];
                }
                else {
                    // 1 + minimum of cell to the left, to the top, diagonally left and up
                    d[i] = 1 + Math.min(Math.min(d[i - 1], p[i]), p[i - 1]);
                }
            }

            // copy current distance counts to 'previous row' distance counts
            _d = p;
            p = d;
            d = _d;
        }

        // if p[n] is greater than the threshold, there's no guarantee on it being the correct
        // distance
        if (p[n] <= threshold) {
            return p[n];
        }
        return -1;
    }

    public static List<String> findSimiliar(CharSequence match, Collection<String> potential) {
        List<String> list = new ArrayList<String>(3);

        // 1 switches or 1 extra char
        int maxDistance = 2;

        for (String string : potential) {
            int dist = levenshteinDistance(match, string, maxDistance);
            if (dist >= 0) {
                if (dist < maxDistance) {
                    maxDistance = dist;
                    list.clear();
                    list.add(string);
                }
                else if (dist == maxDistance) {
                    list.add(string);
                }
            }
        }

        return list;
    }

    public static String sanitizeResource(String resource) {
        String res = resource.trim();
        if (res.startsWith("/")) {
            res = res.substring(1);
        }
        if (res.endsWith("/")) {
            res = res.substring(0, res.length() - 1);
        }
        return res;
    }

    private static final char[] SINGLE_INDEX_ILLEGAL_START_CHARACTERS = {'_', '+', '-'};
    private static final char[] SINGLE_INDEX_ILLEGAL_CHARACTERS = {' ', '\"', '*', '\\', '<', '|', ',', '>', '/', '?'};

    /**
     * Determines if a string is a valid name for a singular index in that
     * it contains no illegal index name characters, and would likely be legal
     * for use with API's that operate on singular indices only (writes, etc)
     */
    public static boolean isValidSingularIndexName(String singleIndexName) {
        boolean firstRun = true;
        char[] chars = singleIndexName.toCharArray();
        for (int idx = 0; idx < chars.length; idx++) {
            char c = chars[idx];
            // Check first character for illegal starting chars
            if (firstRun) {
                for (char illegalStartCharacter : SINGLE_INDEX_ILLEGAL_START_CHARACTERS) {
                    if (c == illegalStartCharacter) {
                        return false;
                    }
                }
                firstRun = false;
            }
            // Check for any illegal chars
            for (char illegalCharacter : SINGLE_INDEX_ILLEGAL_CHARACTERS) {
                if (c == illegalCharacter) {
                    return false;
                }
            }
            // No uppercase characters
            if (Character.isHighSurrogate(c)) {
                // Be sensitive to surrogate pairs in unicode...
                int hi = (int) c;
                int lo = (int) chars[++idx];
                int codePoint = ((hi - 0xD800) * 0x400) + (lo - 0XDC00) + 0x10000;
                if (Character.isUpperCase(codePoint)) {
                    return false;
                }
            } else if (Character.isUpperCase(c)) {
                return false;
            }
        }
        return true;
    }

    public static boolean isLowerCase(CharSequence string) {
        for (int index = 0; index < string.length(); index++) {
            if (Character.isUpperCase(string.charAt(index))) {
                return false;
            }
        }
        return true;
    }

    public static boolean hasLetter(CharSequence string) {
        for (int index = 0; index < string.length(); index++) {
            if (Character.isLetter(string.charAt(index))) {
                return true;
            }
        }
        return false;
    }

    public static String jsonEncoding(String rawString) {
        return new String(HAS_JACKSON_CLASS ? JacksonStringEncoder.jsonEncoding(rawString) : BackportedJsonStringEncoder.getInstance().quoteAsString(rawString));
    }

    // return the value in a JSON friendly way
    public static String toJsonString(Object value) {
        if (value == null) {
            return "null";
        }
        else if (value.getClass().equals(String.class)) {
            return "\"" + StringUtils.jsonEncoding(value.toString()) + "\"";
        }
        // else it's a RawJson, Boolean or Number so no escaping or quotes
        else {
            return value.toString();
        }
    }

    private static class JacksonStringEncoder {
        public static char[] jsonEncoding(String rawString) {
            return JsonStringEncoder.getInstance().quoteAsString(rawString);
        }
    }

    public static IpAndPort parseIpAddress(String httpAddr) {
        // strip ip address - regex would work but it's overkill

        // there are four formats - ip:port, hostname/ip:port or [/ip:port] and [hostname/ip:port]
        // first the ip is normalized
        if (httpAddr.contains("/")) {
            int startIp = httpAddr.indexOf("/") + 1;
            int endIp = httpAddr.indexOf("]");
            if (endIp < 0) {
                endIp = httpAddr.length();
            }
            if (startIp < 0) {
                throw new EsHadoopIllegalStateException("Cannot parse http address " + httpAddr);
            }
            httpAddr = httpAddr.substring(startIp, endIp);
        }

        // then split
        int portIndex = httpAddr.lastIndexOf(":");

        if (portIndex > 0) {
            String ip = httpAddr.substring(0, portIndex);
            int port = Integer.valueOf(httpAddr.substring(portIndex + 1));
            return new IpAndPort(ip, port);
        }
        return new IpAndPort(httpAddr);
    }

    public static String stripFieldNameSourcePrefix(String fieldName) {
        if (fieldName != null) {
            if (fieldName.startsWith(SOURCE_FIELD_NAME)) {
                return fieldName.substring(SOURCE_FIELD_NAME.length());
            } else if (fieldName.startsWith(SOURCE_ROOT)) {
                return fieldName.substring(SOURCE_ROOT.length());
            } else if (fieldName.startsWith(FIELD_FIELD_NAME)) {
                return fieldName.substring(FIELD_FIELD_NAME.length());
            } else if (fieldName.startsWith(FIELDS_ROOT)) {
                return fieldName.substring(FIELDS_ROOT.length());
            }
        }
        return fieldName;
    }
}