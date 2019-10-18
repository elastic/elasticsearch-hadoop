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

import org.elasticsearch.hadoop.rest.query.BoolQueryBuilder;
import org.elasticsearch.hadoop.rest.query.FilteredQueryBuilder;
import org.elasticsearch.hadoop.rest.query.MatchAllQueryBuilder;
import org.elasticsearch.hadoop.rest.query.QueryBuilder;
import org.elasticsearch.hadoop.serialization.ScrollReader;
import org.elasticsearch.hadoop.serialization.json.JacksonJsonGenerator;
import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.BytesArray;
import org.elasticsearch.hadoop.util.EsMajorVersion;
import org.elasticsearch.hadoop.util.FastByteArrayOutputStream;
import org.elasticsearch.hadoop.util.StringUtils;
import org.elasticsearch.hadoop.util.encoding.HttpEncodingTools;
import org.elasticsearch.hadoop.util.unit.TimeValue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * A search request builder which allows building {@link ScrollQuery}
 */
public class SearchRequestBuilder {
    private static class Slice {
        final int id;
        final int max;

        Slice(int id, int max) {
            this.id = id;
            this.max = max;
        }
    }

    private final EsMajorVersion version;
    private final boolean includeVersion;
    private TimeValue scroll = TimeValue.timeValueMinutes(10);
    private long size = 50;
    private long limit = -1;
    private String indices;
    private String types;
    private String shard;
    private String fields;
    private QueryBuilder query;
    private final List<QueryBuilder> filters = new ArrayList<QueryBuilder> ();
    private String routing;
    private Slice slice;
    private boolean local = false;
    private String preference = "";
    private boolean excludeSource = false;
    private boolean readMetadata = false;

    public SearchRequestBuilder(EsMajorVersion version, boolean includeVersion) {
        this.version = version;
        this.includeVersion = includeVersion;
    }

    public boolean isReadMetadata() {
        return readMetadata;
    }

    public String routing() {
        return routing;
    }

    public List<QueryBuilder> filters() {
        return filters;
    }

    public SearchRequestBuilder resource(Resource resource) {
        this.indices = resource.index();
        if (resource.isTyped()) {
            this.types = resource.type();
        } else {
            this.types = null;
        }
        return this;
    }

    public SearchRequestBuilder indices(String indices) {
        this.indices = indices;
        return this;
    }

    public SearchRequestBuilder query(QueryBuilder builder) {
        this.query = builder;
        return this;
    }

    public QueryBuilder query() {
        return query;
    }

    public SearchRequestBuilder size(long size) {
        this.size = size;
        return this;
    }

    public SearchRequestBuilder limit(long limit) {
        this.limit = limit;
        return this;
    }

    public SearchRequestBuilder scroll(long keepAliveMillis) {
        Assert.isTrue(keepAliveMillis > 0, "Invalid scroll");
        this.scroll = TimeValue.timeValueMillis(keepAliveMillis);
        return this;
    }

    public SearchRequestBuilder shard(String shard) {
        Assert.hasText(shard, "Invalid shard");
        this.shard = shard;
        return this;
    }

    public SearchRequestBuilder fields(String fieldsCSV) {
        Assert.isFalse(this.excludeSource, "Fields can't be requested because _source section is excluded");
        this.fields = fieldsCSV;
        return this;
    }

    public SearchRequestBuilder filter(QueryBuilder filter) {
        this.filters.add(filter);
        return this;
    }

    public SearchRequestBuilder filters(Collection<QueryBuilder> filters) {
        this.filters.addAll(filters);
        return this;
    }

    public SearchRequestBuilder readMetadata(boolean read) {
        this.readMetadata = read;
        return this;
    }

    public SearchRequestBuilder routing(String routing) {
        this.routing = routing;
        return this;
    }

    public SearchRequestBuilder slice(int id, int max) {
        this.slice = new Slice(id, max);
        return this;
    }

    public SearchRequestBuilder local(boolean value) {
        this.local = value;
        return this;
    }

    public SearchRequestBuilder preference(String preference) {
        this.preference = preference;
        return this;
    }

    public SearchRequestBuilder excludeSource(boolean value) {
        if (value) {
            Assert.hasNoText(this.fields, String.format("_source section can't be excluded if fields [%s] are requested", this.fields));
        }
        this.excludeSource = value;
        return this;
    }

    private String assemble() {
        if (limit > 0) {
            if (size > limit) {
                size = limit;
            }
        }
        Map<String, String> uriParams = new LinkedHashMap<String, String>();
        StringBuilder sb = new StringBuilder();
        sb.append(indices);
        if (StringUtils.hasLength(types)) {
            sb.append("/");
            sb.append(types);
        }
        sb.append("/_search?");

        // override infrastructure params
        if (version.onOrAfter(EsMajorVersion.V_5_X)) {
            // scan type was removed
            // default to sorting by indexing/doc order
            uriParams.put("sort", "_doc");
        }
        else {
            uriParams.put("search_type", "scan");
        }
        uriParams.put("scroll", String.valueOf(scroll.toString()));
        uriParams.put("size", String.valueOf(size));
        if (includeVersion) {
            uriParams.put("version", "true");
        }

        // set shard preference
        StringBuilder pref = new StringBuilder();
        if (StringUtils.hasText(shard)) {
            pref.append("_shards:");
            pref.append(shard);
        }
        if (local || StringUtils.hasText(preference)) {
            if (pref.length() > 0) {
                if (version.onOrAfter(EsMajorVersion.V_5_X)) {
                    pref.append("|");
                } else {
                    pref.append(";");
                }
            }
            if (StringUtils.hasText(preference)) {
                pref.append(preference);
            } else {
                pref.append("_local");
            }
        }

        if (pref.length() > 0) {
            uriParams.put("preference", HttpEncodingTools.encode(pref.toString()));
        }

        // Request routing
        if (routing != null) {
            uriParams.put("routing", HttpEncodingTools.encode(routing));
        }

        // Always track total hits on versions that support it. 7.0+ will return lower bounded
        // hit counts if this is not set, and we want them to be accurate for scroll bookkeeping.
        if (version.onOrAfter(EsMajorVersion.V_6_X)) {
            uriParams.put("track_total_hits", "true");
        }

        if (readMetadata) {
            uriParams.put("track_scores", "true");
        }

        // append params
        for (Iterator<Entry<String, String>> it = uriParams.entrySet().iterator(); it.hasNext();) {
            Entry<String, String> entry = it.next();
            sb.append(entry.getKey());
            if (StringUtils.hasText(entry.getValue())) {
                sb.append("=");
                sb.append(entry.getValue());
            }
            if (it.hasNext()) {
                sb.append("&");
            }
        }

        return sb.toString();
    }

    private BytesArray assembleBody() {
        QueryBuilder root = query;
        if (root == null) {
            root = MatchAllQueryBuilder.MATCH_ALL;
        }
        if (filters.isEmpty() == false) {
            if (version.onOrAfter(EsMajorVersion.V_2_X)) {
                root = new BoolQueryBuilder().must(root).filters(filters);
            } else {
                root = new FilteredQueryBuilder().query(root).filters(filters);
            }
        }
        FastByteArrayOutputStream out = new FastByteArrayOutputStream(256);
        JacksonJsonGenerator generator = new JacksonJsonGenerator(out);
        try {
            generator.writeBeginObject();
            if (slice != null && slice.max > 1) {
                generator.writeFieldName("slice");
                generator.writeBeginObject();
                generator.writeFieldName("id");
                generator.writeNumber(slice.id);
                generator.writeFieldName("max");
                generator.writeNumber(slice.max);
                generator.writeEndObject();
            }
            generator.writeFieldName("query");
            generator.writeBeginObject();
            root.toJson(generator);
            generator.writeEndObject();
            // override fields
            if (StringUtils.hasText(fields)) {
                generator.writeFieldName("_source");
                generator.writeBeginArray();
                final List<String> fieldsArray = StringUtils.tokenize(fields, StringUtils.DEFAULT_DELIMITER);
                for (String field : fieldsArray) {
                    generator.writeString(field);
                }
                generator.writeEndArray();
            } else if (excludeSource) {
                generator.writeFieldName("_source");
                generator.writeBoolean(false);
            }
            generator.writeEndObject();
        } finally {
            generator.close();
        }
        return out.bytes();
    }

    public ScrollQuery build(RestRepository client, ScrollReader reader) {
        String scrollUri = assemble();
        BytesArray requestBody = assembleBody();
        return client.scanLimit(scrollUri, requestBody, limit, reader);
    }

    @Override
    public String toString() {
        return "QueryBuilder [" + assemble() + "][" + assembleBody() + "]";
    }
}
