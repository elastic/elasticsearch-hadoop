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
 * ElasticSearch Resource (index or query).
 */
class Resource {

    private final String resource;
    // cleaned up index with trailing "/"
    private final String root;
    private boolean isQuery;

    Resource(String resource) {
        this.resource = resource;
        int index = resource.lastIndexOf("_");
        if (index <= 0) {
            index = resource.length();
        }
        String localRoot = resource.substring(0, index);
        if (!localRoot.endsWith("/")) {
            localRoot = localRoot + "/";
        }
        root = localRoot;
    }

    boolean isQuery() {
        return isQuery;
    }

    boolean isIndex() {
        return !isQuery;
    }

    String bulkIndexing() {
        return root + "_bulk";
    }

    String shardInfo() {
        return root + "_search_shards";
    }
}
