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

import java.util.ArrayList;
import java.util.Collection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.elasticsearch.hadoop.util.StringUtils;

class ErrorUtils {

    // find error inside ElasticsearchParseException
    private static final Pattern XCONTENT_PAYLOAD = Pattern.compile("ElasticSearchParseException.+: \\[(.+)\\]");

    static String extractInvalidXContent(String errorMessage) {
        if (!StringUtils.hasText(errorMessage)) {
            return null;
        }

        Matcher matcher = XCONTENT_PAYLOAD.matcher(errorMessage);
        if (!matcher.find()) {
            return null;
        }
        String group = matcher.group(1);
        Collection<Byte> bytes = new ArrayList<Byte>();
        // parse the collection into numbers and back to a String
        try {
            for (String byteValue : StringUtils.tokenize(group, ",")) {
                bytes.add(Byte.parseByte(byteValue));
            }
            byte[] primitives = new byte[bytes.size()];
            int index = 0;
            for (Byte b : bytes) {
                primitives[index++] = b.byteValue();
            }
            return new String(primitives);
        } catch (Exception ex) {
            // can't convert back the byte array - give up
            return null;
        }
    }
}
