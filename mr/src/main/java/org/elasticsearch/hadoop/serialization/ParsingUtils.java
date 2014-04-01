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
package org.elasticsearch.hadoop.serialization;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.serialization.Parser.Token;
import org.elasticsearch.hadoop.util.StringUtils;

public abstract class ParsingUtils {

    /**
     * Seeks the field with the given name in the stream and positions (and returns) the parser to the next available token (value or not).
     * Return null if no token is found.
     *
     * @param path
     * @param parser
     * @return token associated with the given path or null if not found
     */
    public static Token seek(String path, Parser parser) {
        // return current token if no path is given
        if (!StringUtils.hasText(path)) {
            return null;
        }

        List<String> tokens = StringUtils.tokenize(path, ".");
        return seek(parser, tokens.toArray(new String[tokens.size()]));
    }

    public static Token seek(Parser parser, String[] path1) {
        return seek(parser, path1, null);
    }

    public static Token seek(Parser parser, String[] path1, String[] path2) {
        return doSeekToken(parser, path1, 0, path2, 0);
    }

    private static Token doSeekToken(Parser parser, String[] path1, int index1, String[] path2, int index2) {
        Token token = null;

        String currentName;
        token = parser.currentToken();
        if (token == null) {
            token = parser.nextToken();
        }

        while ((token = parser.nextToken()) != null) {
            if (token == Token.START_OBJECT) {
                token = parser.nextToken();
            }
            if (token == Token.FIELD_NAME) {
                // found a node, go one level deep
                currentName = parser.currentName();
                if (path1 != null && currentName.equals(path1[index1])) {
                    if (index1 + 1 < path1.length) {
                        return doSeekToken(parser, path1, index1 + 1, null, 0);
                    }
                    else {
                        return parser.nextToken();
                    }
                }
                else if (path2 != null && currentName.equals(path2[index2])) {
                    if (index2 + 1 < path2.length) {
                        return doSeekToken(parser, null, 0, path2, index2 + 1);
                    }
                    else {
                        return parser.nextToken();
                    }
                }
                else {
                    // get field token (can be value, object or array)
                    parser.nextToken();
                    parser.skipChildren();
                }
            }
            else {
                break;
            }
        }

        return null;
    }

    private static class Matcher {
        private final List<String> tokens;
        private int tokenIndex = 0;
        private boolean matched = false;
        private Object value;

        Matcher(String path) {
            tokens = StringUtils.tokenize(path, ".");
        }

        boolean matches(String value) {
            boolean match = tokens.get(tokenIndex).equals(value);
            if (match) {
                if (tokenIndex < tokens.size() - 1) {
                    tokenIndex++;
                }
                else {
                    matched = true;
                    this.value = value;
                }
            }
            return match;
        };
    }

    public static List<String> values(Parser parser, String... paths) {
        List<Matcher> matchers = new ArrayList<Matcher>(paths.length);
        for (String path : paths) {
            matchers.add(new Matcher(path));
        }

        List<Matcher> active = new ArrayList<Matcher>(matchers);
        Set<Matcher> inactive = new LinkedHashSet<Matcher>();

        doFind(parser, new ArrayList<Matcher>(matchers), active, inactive);

        List<String> matches = new ArrayList<String>();
        for (Matcher matcher : matchers) {
            matches.add(matcher.matched ? matcher.value.toString() : null);
        }

        return matches;
    }

    private static void doFind(Parser parser, List<Matcher> current, List<Matcher> active, Set<Matcher> inactive) {
        Token token = null;
        List<Matcher> matchingCurrentLevel = null;

        String currentName;
        token = parser.currentToken();
        if (token == null) {
            token = parser.nextToken();
        }

        while ((token = parser.nextToken()) != null) {
            if (token == Token.START_OBJECT) {
                token = parser.nextToken();
                if (matchingCurrentLevel == null) {
                    parser.skipChildren();
                }
                else {
                    doFind(parser, matchingCurrentLevel, active, inactive);
                }
            }
            else if (token == Token.FIELD_NAME) {
                currentName = parser.currentName();

                Object value = null;
                boolean valueRead = false;

                for (Matcher matcher : current) {
                    if (matcher.matches(currentName)) {
                        if (matcher.matched) {
                            inactive.add(matcher);
                            if (!valueRead) {
                                switch (parser.nextToken()) {
                                case VALUE_NUMBER:
                                    value = parser.numberValue();
                                    break;
                                case VALUE_BOOLEAN:
                                    value = Boolean.valueOf(parser.booleanValue());
                                    break;
                                case VALUE_NULL:
                                    value = null;
                                    break;
                                case VALUE_STRING:
                                    value = parser.text();
                                    break;
                                default:
                                    throw new EsHadoopIllegalArgumentException(String.format(
                                            "Incorrect parsing; expected value but found [%s]", parser.currentToken()));
                                }
                            }
                            matcher.value = value;
                        }
                        else {
                            if (matchingCurrentLevel == null) {
                                matchingCurrentLevel = new ArrayList<Matcher>(current.size());
                            }
                            matchingCurrentLevel.add(matcher);
                        }
                    }
                }
            }
            else if (token == Token.END_OBJECT) {
                // once matching, the matcher needs to match all the way - if it's not inactive (since it matched)
                if (matchingCurrentLevel != null) {
                    for (Matcher matcher : matchingCurrentLevel) {
                        active.remove(matcher);
                        inactive.add(matcher);
                    }
                }
            }
            // ignore other tokens
        }
    }
}