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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.util.regex.Regex;

public abstract class FieldFilter {

    public static class Result {
        public final boolean matched;
        public final int depth;

        public Result(boolean matched) {
            this(matched, 1);
        }

        public Result(boolean matched, int depth) {
            this.matched = matched;
            this.depth = depth;
        }
    }

    public static final Result INCLUDED = new Result(true);
    public static final Result EXCLUDED = new Result(false);

    public static class NumberedInclude {
        public final String filter;
        public final int depth;

        public NumberedInclude(String filter) {
            this(filter, 1);
        }

        public NumberedInclude(String filter, int depth) {
            this.filter = filter;
            this.depth = depth;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            NumberedInclude that = (NumberedInclude) o;

            if (depth != that.depth) return false;
            return filter != null ? filter.equals(that.filter) : that.filter == null;
        }

        @Override
        public int hashCode() {
            int result = filter != null ? filter.hashCode() : 0;
            result = 31 * result + depth;
            return result;
        }

        @Override
        public String toString() {
            return "NumberedInclude{"+filter+ ":"+depth+"}";
        }
    }

    /**
     * Returns true if the key should be kept or false if it needs to be skipped/dropped.
     *
     * @param path
     * @param includes
     * @param excludes
     * @return
     */
    public static Result filter(String path, Collection<NumberedInclude> includes, Collection<String> excludes, boolean allowPartialMatches) {
        includes = (includes == null ? Collections.<NumberedInclude> emptyList() : includes);
        excludes = (excludes == null ? Collections.<String> emptyList() : excludes);

        if (includes.isEmpty() && excludes.isEmpty()) {
            return INCLUDED;
        }

        if (Regex.simpleMatch(excludes, path)) {
            return EXCLUDED;
        }

        boolean exactIncludeMatch = false; // true if the current position was specifically mentioned
        boolean pathIsPrefixOfAnInclude = false; // true if potentially a sub scope can be included

        NumberedInclude matchedInclude = null;

        if (includes.isEmpty()) {
            // implied match anything
            exactIncludeMatch = true;
        }
        else {
            for (NumberedInclude filter : includes) {
                matchedInclude = filter;
                String include = filter.filter;

                // check for prefix matches as well to see if we need to zero in, something like: obj1.arr1.* or *.field
                // note, this does not work well with middle matches, like obj1.*.obj3
                if (include.charAt(0) == '*') {
                    if (Regex.simpleMatch(include, path)) {
                        exactIncludeMatch = true;
                        break;
                    }
//                    pathIsPrefixOfAnInclude = true;
//                    continue;
                }
                if (include.startsWith(path)) {
                    if (include.length() == path.length()) {
                        exactIncludeMatch = true;
                        break;
                    }
                    else if (include.length() > path.length() && include.charAt(path.length()) == '.') {
                        // This is mostly for the case that we are writing a document with a path like "a.b.c", we are
                        // starting to write the first field "a", and we have marked in the settings that we want to
                        // explicitly include the field "a.b". We don't want to skip writing field "a" in this case;
                        // Moreover, we want to include "a" because we know that "a.b" was explicitly included to be
                        // written, and we can't determine yet if "a.b" even exists at this point.
                        // The same logic applies for reading data. Make sure to read the "a" field even if it's not
                        // marked as included specifically. If we skip the "a" field at this point, we may skip the "b"
                        // field underneath it which is marked as explicitly included.
                        pathIsPrefixOfAnInclude = true;
                        continue;
                    }
                }
                if (Regex.simpleMatch(include, path)) {
                    exactIncludeMatch = true;
                    break;
                }
            }
        }

        // if match or part of the path (based on the passed param)
        if (exactIncludeMatch || (allowPartialMatches && pathIsPrefixOfAnInclude)) {
            return (matchedInclude != null ? new Result(true, matchedInclude.depth) : INCLUDED);
        }

        return EXCLUDED;
    }

    public static Result filter(String path, Collection<NumberedInclude> includes, Collection<String> excludes) {
        return filter(path, includes, excludes, true);
    }

    public static List<NumberedInclude> toNumberedFilter(Collection<String> includeAsStrings){
        if (includeAsStrings == null || includeAsStrings.isEmpty()) {
            return Collections.<NumberedInclude> emptyList();
        }

        List<NumberedInclude> numberedIncludes = new ArrayList<NumberedInclude>(includeAsStrings.size());

        for (String include : includeAsStrings) {
            int index = include.indexOf(":");
            String filter = include;
            int depth = 1;

            try {
                if (index > 0) {
                    filter = include.substring(0, index);
                    String depthString = include.substring(index + 1);
                    if (depthString.length() > 0) {
                        depth = Integer.parseInt(depthString);
                    }
                }
            } catch (NumberFormatException ex) {
                throw new EsHadoopIllegalArgumentException(
                    String.format(Locale.ROOT, "Invalid parameter [%s] specified in inclusion configuration", include),
                    ex
                );
            }
            numberedIncludes.add(new NumberedInclude(filter, depth));
        }

        return numberedIncludes;
    }
}