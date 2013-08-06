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

import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.serialization.ScrollReader;
import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.StringUtils;
import org.elasticsearch.hadoop.util.unit.TimeValue;

public class QueryBuilder {

    private final String query;
    private TimeValue time = TimeValue.timeValueMinutes(10);
    private long size = 50;
    private String shard;
    private String node;

    private QueryBuilder(String query) {
        Assert.hasText(query, "Invalid query");
        this.query = query;
    }

    public static QueryBuilder query(Settings settings) {
        return new QueryBuilder(settings.getTargetResource()).time(settings.getScrollKeepAlive()).
                                size(settings.getScrollSize());
    }
    public static QueryBuilder query(String query) {
        return new QueryBuilder(query);
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
        StringBuilder sb = new StringBuilder();
        sb.append(query);
        sb.append("&search_type=scan&scroll=");
        sb.append(time.minutes());
        sb.append("m&size=");
        sb.append(size);

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
            sb.append("&preference=");
            sb.append(pref.toString());
        }

        return sb.toString();
    }

    public ScrollQuery build(BufferedRestClient client, ScrollReader reader) {
        String scrollUri = assemble();
        try {
            return client.scan(scrollUri, reader);
        } catch (IOException ex) {
            throw new IllegalStateException("Cannot build scroll [" + scrollUri + "]", ex);
        }
    }

    @Override
    public String toString() {
        return "QueryBuilder [" + assemble() + "]";
    }
}