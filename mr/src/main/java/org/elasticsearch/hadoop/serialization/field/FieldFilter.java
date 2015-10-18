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

import java.util.Collection;
import java.util.Collections;

import org.elasticsearch.hadoop.util.regex.Regex;

public abstract class FieldFilter {

    /**
     * Returns true if the key should be kept or false if it needs to be skipped/dropped.
     *
     * @param path
     * @param includes
     * @param excludes
     * @return
     */
    public static boolean filter(String path, Collection<String> includes, Collection<String> excludes, boolean allowPartialMatches) {
        includes = (includes == null ? Collections.<String> emptyList() : includes);
        excludes = (excludes == null ? Collections.<String> emptyList() : excludes);

        if (includes.isEmpty() && excludes.isEmpty()) {
            return true;
        }

        if (Regex.simpleMatch(excludes, path)) {
            return false;
        }

        boolean exactIncludeMatch = false; // true if the current position was specifically mentioned
        boolean pathIsPrefixOfAnInclude = false; // true if potentially a sub scope can be included
        if (includes.isEmpty()) {
            // implied match anything
            exactIncludeMatch = true;
        }
        else {
            for (String include : includes) {
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
                        // include might may match deeper paths. Dive deeper.
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
            return true;
        }

        return false;
    }

    public static boolean filter(String path, Collection<String> includes, Collection<String> excludes) {
        return filter(path, includes, excludes, true);
    }
}