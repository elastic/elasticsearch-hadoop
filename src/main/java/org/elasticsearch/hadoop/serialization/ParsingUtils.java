/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticsearch.hadoop.serialization;

import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.elasticsearch.hadoop.rest.dto.mapping.Field;
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

        List<String> tokens = StringUtils.tokenize(path, "/");
        ListIterator<String> li = tokens.listIterator();
        return doSeekToken(li.next(), li, parser);
    }

    private static Token doSeekToken(String targetNode, ListIterator<String> listIterator, Parser parser) {
        Token token = null;

        while ((token = parser.nextToken()) != null) {
            if (token == Token.FIELD_NAME) {
                // found a node, go one level deep
                if (targetNode.equals(parser.currentName())) {
                    if (listIterator.hasNext()) {
                        return doSeekToken(listIterator.next(), listIterator, parser);
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
        }
        return null;
    }

    static void add(Map<String, FieldType> fields, Field field, String parentName) {
        if (FieldType.OBJECT == field.type()) {
            if (parentName != null) {
                parentName = parentName + "\\" + field.name();
            }
            for (Field nestedField : field.properties()) {
                add(fields, nestedField, parentName);
            }
        }
        else {
            fields.put(field.name(), field.type());
        }
    }
}
