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

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.serialization.ScrollReader;
import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.BytesArray;
import org.elasticsearch.hadoop.util.IOUtils;
import org.elasticsearch.hadoop.util.StringUtils;
import org.elasticsearch.hadoop.util.unit.TimeValue;

public class QueryBuilder {

    private final Resource resource;

    private static String MATCH_ALL = "{\"query\":{\"match_all\":{}}}";

    private Map<String, String> uriQuery = new LinkedHashMap<String, String>();
    private BytesArray bodyQuery;

    private TimeValue time = TimeValue.timeValueMinutes(10);
    private long size = 50;
    private String shard;
    private String node;

    QueryBuilder(Settings settings) {
        this.resource = new Resource(settings);
        String query = settings.getQuery();
        if (!StringUtils.hasText(query)) {
            query = MATCH_ALL;
        }
        parseQuery(query.trim());
    }

    QueryBuilder(Resource resource, String query) {
        this.resource = resource;
        Assert.hasText(query, "no/empty query was given");
        parseQuery(query.trim());
    }

    public static QueryBuilder query(Settings settings) {
        return new QueryBuilder(settings).
                        time(settings.getScrollKeepAlive()).
                        size(settings.getScrollSize());
    }


    private void parseQuery(String query) {
        // uri query
        if (query.startsWith("?")) {
            uriQuery.putAll(initUriQuery(query));
        }
        else if (query.startsWith("{")) {
            // TODO: add early, basic JSON validation
            bodyQuery = new BytesArray(query);
        }
        else {
            try {
                // must be a resource
                InputStream in = IOUtils.open(query);
                // peek the stream
                int first = in.read();
                if ('q' == first) {
                    uriQuery.putAll(initUriQuery(IOUtils.asString(in)));
                }
                else {
                    bodyQuery = new BytesArray(1024);
                    bodyQuery.add(first);
                    IOUtils.asBytes(bodyQuery, in);
                }
            } catch (IOException ex) {
                throw new IllegalStateException(ex);
            }
        }
    }

    private Map<String, String> initUriQuery(String query) {
        // strip leading ?
        if (query.startsWith("?")) {
            query = query.substring(1);
        }
        Map<String, String> params = new LinkedHashMap<String, String>();
        for (String token : query.split("&")) {
            int indexOf = token.indexOf("=");
            Assert.isTrue(indexOf > 0, String.format("Cannot token [%s] in uri query [%s]", token, query));
            params.put(token.substring(0, indexOf), token.substring(indexOf + 1));
        }
        return params;
    }

    public QueryBuilder size(long size) {
        this.size = size;
        return this;
    }

    public QueryBuilder time(long timeInMillis) {
        Assert.isTrue(timeInMillis > 0, "Invalid time");
        this.time = TimeValue.timeValueMillis(timeInMillis);
        return this;
    }

    public QueryBuilder onlyNode(String node) {
        Assert.hasText(node, "Invalid node");
        this.node = node;
        return this;
    }

    public QueryBuilder shard(String shard) {
        Assert.hasText(shard, "Invalid shard");
        this.shard = shard;
        return this;
    }

    private String assemble() {
        StringBuilder sb = new StringBuilder(resource.indexAndType());
        sb.append("/_search?");

        // override infrastructure params
        uriQuery.put("search_type", "scan");
        uriQuery.put("scroll", String.valueOf(time.minutes()));
        uriQuery.put("size", String.valueOf(size));

        StringBuilder pref = new StringBuilder();
        if (StringUtils.hasText(shard)) {
            pref.append("_shards:");
            pref.append(shard);
        }
        if (StringUtils.hasText(node)) {
            if (pref.length() > 0) {
                pref.append(";");
            }
            pref.append("_only_node:");
            pref.append(node);
        }

        if (pref.length() > 0) {
            uriQuery.put("preference", pref.toString());
        }

        // append params
        for (Iterator<Entry<String, String>> it = uriQuery.entrySet().iterator(); it.hasNext();) {
            Entry<String, String> entry = it.next();
            sb.append(entry.getKey());
            sb.append("=");
            sb.append(entry.getValue());
            if (it.hasNext()) {
                sb.append("&");
            }
        }

        return sb.toString();
    }

    public ScrollQuery build(BufferedRestClient client, ScrollReader reader) {
        String scrollUri = assemble();
        try {
            return client.scan(scrollUri, bodyQuery, reader);
        } catch (IOException ex) {
            throw new IllegalStateException("Cannot build scroll [" + scrollUri + "]", ex);
        }
    }

    @Override
    public String toString() {
        return "QueryBuilder [" + assemble() + "]";
    }
}