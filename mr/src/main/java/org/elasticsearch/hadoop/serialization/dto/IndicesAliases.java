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
package org.elasticsearch.hadoop.serialization.dto;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class IndicesAliases {
    private final Map<String, Map<String, Alias> > indices;

    private IndicesAliases(Map<String, Map<String, Alias> > indices) {
        this.indices = indices;
    }

    public Map<String, Alias> getAliases(String index) {
        return indices.get(index);
    }

    /**
     * Parse an aliases response into an instance of {@link IndicesAliases}
     *
     * Example of response from server:
     * <pre>
     * {
     *   "index1" : {
     *     "aliases" : {
     *       "alias1" : {
     *         "filter" : {
     *           "term" : {
     *             "user" : "kimchy"
     *           }
     *         },
     *         "index_routing" : "1",
     *         "search_routing" : "1"
     *       },
     *       "alias2" : {
     *         "search_routing" : "5"
     *       }
     *     }
     *   },
     *   "index2" : {
     *     "aliases" : {
     *       ...
     *     }
     *   }
     * }
     * </pre>
     *
     * @param resp JSON Response in the form of a Java Map
     */
    public static IndicesAliases parse(Map<String, Object> resp) {
        final Map<String, Map<String, Alias> > indices = new HashMap<String, Map<String, Alias> > ();
        for (Map.Entry<String, Object> index : resp.entrySet()) {
            final Map<String, Object> metadata = (Map<String, Object>) index.getValue();
            final Map<String, Map<String, Object> > aliases = (Map<String, Map<String, Object> >) metadata.get("aliases");
            final Map<String, Alias> indexAliases = new HashMap<String, Alias> ();
            indices.put(index.getKey(), indexAliases);
            for (Map.Entry<String, Map<String, Object> > entry : aliases.entrySet()) {
                String name = entry.getKey();
                Map<String, Object> aliasMetadata = entry.getValue();
                String searchRouting = null;
                String indexRouting = null;
                Map<String, Object> filter = null;

                if (aliasMetadata.containsKey("search_routing")) {
                    searchRouting = (String) aliasMetadata.get("search_routing");
                }
                if (aliasMetadata.containsKey("index_routing")) {
                    indexRouting = (String) aliasMetadata.get("index_routing");
                }
                if (aliasMetadata.containsKey("filter")) {
                    filter = (Map<String, Object>) aliasMetadata.get("filter");
                }
                Alias alias = new Alias(name, searchRouting, indexRouting, filter);
                indexAliases.put(alias.name, alias);
            }
        }
        return new IndicesAliases(Collections.unmodifiableMap(indices));
    }


    public static class Alias {
        private final String name;
        private final String searchRouting;
        private final String indexRouting;
        private final Map<String, Object> filter;


        Alias(String name, String searchRouting, String indexRouting, Map<String, Object> filter) {
            this.name = name;
            this.searchRouting = searchRouting;
            this.indexRouting = indexRouting;
            this.filter = filter;
        }

        public String getName() {
            return name;
        }

        public String getSearchRouting() {
            return searchRouting;
        }

        public String getIndexRouting() {
            return indexRouting;
        }

        public Map<String, Object> getFilter() {
            return filter;
        }

        @Override
        public String toString() {
            return "Alias{" +
                    "name='" + name + '\'' +
                    ", searchRouting='" + searchRouting + '\'' +
                    ", indexRouting='" + indexRouting + '\'' +
                    ", filter=" + filter +
                    '}';
        }
    }
}
