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
package org.elasticsearch.hadoop.rest.dto.mapping;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.cfg.FieldPresenceValidation;
import org.elasticsearch.hadoop.serialization.FieldType;
import org.elasticsearch.hadoop.util.StringUtils;

@SuppressWarnings("rawtypes")
public class MappingUtils {

    public static void validateMapping(String fields, Field mapping, FieldPresenceValidation validation, Log log) {
        if (StringUtils.hasText(fields)) {
            validateMapping(StringUtils.tokenize(fields), mapping, validation, log);
        }
    }

    public static void validateMapping(Collection<String> fields, Field mapping, FieldPresenceValidation validation, Log log) {
        if (mapping == null || fields == null || fields.isEmpty() || validation == null || FieldPresenceValidation.IGNORE == validation) {
            return;
        }

        List[] results = findFixes(fields, mapping);

        if (results == null) {
            return;
        }

        String message = String.format("Field(s) [%s]  not found in the Elasticsearch mapping specified; did you mean [%s]?",
                removeDoubleBrackets(results[0]), removeDoubleBrackets(results[1]));
        if (validation == FieldPresenceValidation.WARN) {
            log.warn(message);
        }
        else {
            throw new EsHadoopIllegalArgumentException(message);
        }
    }

    // return a tuple for proper messages
    static List[] findFixes(Collection<String> fields, Field mapping) {
        Map<String, FieldType> map = Field.toLookupMap(mapping);
        // find missing
        List<String> missing = new ArrayList<String>(fields.size());

        for (String field : fields) {
            if (!map.containsKey(field)) {
                missing.add(field);
            }
        }
        if (missing.isEmpty()) {
            return null;
        }
        Map<String, String> unwrapped = new LinkedHashMap<String, String>();
        // find similar
        for (Map.Entry<String, FieldType> entry : map.entrySet()) {
            String key = entry.getKey();
            int match = key.lastIndexOf(".");
            if (match > 0) {
                String leafField = key.substring(match + 1);
                // leaf fields are secondary to top-level ones (in case of overwrite, the top level ones win)
                if (!unwrapped.containsKey(leafField)) {
                    unwrapped.put(leafField, key);
                }
            }
            unwrapped.put(key, key);
        }

        List<String> typos = new ArrayList<String>();

        Set<String> similar = unwrapped.keySet();
        for (String string : missing) {
            List<String> matches = StringUtils.findSimiliar(string, similar);
            for (String match : matches) {
                // get actual field
                typos.add(unwrapped.get(match));
            }
        }

        return new List[] { missing, typos };
    }

    private static String removeDoubleBrackets(List col) {
        if (col.isEmpty()) {
            return "<no-similar-match-found>";
        }
        if (col.size() == 1) {
            return col.get(0).toString();
        }
        return col.toString();
    }
}
