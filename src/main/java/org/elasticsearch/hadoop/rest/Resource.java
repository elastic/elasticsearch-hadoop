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
package org.elasticsearch.hadoop.rest;


/**
 * ElasticSearch Rest Resource (index or query).
 */
public class Resource {

    private final StringBuilder resource;
    // cleaned up index and type with trailing "/"
    private final String root;
    private final String type;
    private final String index;

    public Resource(String resource) {
        this.resource = new StringBuilder(resource);
        int location = resource.lastIndexOf("_");
        if (location <= 0) {
            location = resource.length();
        }
        String localRoot = resource.substring(0, location);
        if (!localRoot.endsWith("/")) {
            localRoot = localRoot + "/";
        }
        root = localRoot;
        location = localRoot.substring(0, root.length() - 1).lastIndexOf("/");
        type = root.substring(location + 1, root.length() - 1);
        index = root.substring(0, location);
    }

    String bulkIndexing() {
        return root + "_bulk";
    }

    // https://github.com/elasticsearch/elasticsearch/issues/2726
    String targetShards() {
        return root + "_search_shards";
    }

    String mapping() {
        return root + "_mapping";
    }

    String indexAndType() {
        return root;
    }

    public String type() {
        return type;
    }

    public String index() {
        return index;
    }
}

