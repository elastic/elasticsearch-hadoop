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

import org.apache.commons.logging.LogFactory;
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.StringUtils;


/**
 * ElasticSearch Rest Resource - index and type.
 */
public class Resource {

    private final String indexAndType;
    private final String type;
    private final String index;
    private final String bulk;
    private final String refresh;

    public Resource(Settings settings, boolean read) {
        String resource = (read ? settings.getResourceRead() : settings.getResourceWrite());

        String errorMessage = "invalid resource given; expecting [index]/[type]";
        Assert.hasText(resource, errorMessage);

        // add compatibility for now
        if (resource.contains("?") || resource.contains("&")) {
            if (StringUtils.hasText(settings.getQuery())) {
                throw new EsHadoopIllegalArgumentException(String.format(
                        "Cannot specify a query in the target index and through %s", ConfigurationOptions.ES_QUERY));
            }

            LogFactory.getLog(Resource.class).warn(
                    String.format(
                            "queries should be specified through '%s' option and not the target index; this option will be _removed_ in the next release",
                            ConfigurationOptions.ES_QUERY));

            // extract query
            int index = resource.indexOf("?");
            if (index > 0) {
                String query = resource.substring(index);

                // clean resource
                resource = resource.substring(0, index);
                index = resource.lastIndexOf("/");
                Assert.isTrue(index >= 0 && index < resource.length() - 1, errorMessage);
                resource = resource.substring(0, index);

                settings.setProperty(ConfigurationOptions.ES_RESOURCE, resource);
                settings.setQuery(query);
            }
        }

        String res = StringUtils.sanitizeResource(resource);

        int slash = res.indexOf("/");
        Assert.isTrue(slash >= 0 && slash < res.length() - 1, errorMessage);
        index = res.substring(0, slash);
        type = res.substring(slash + 1);

        Assert.hasText(index, "No index found; expecting [index]/[type]");
        Assert.hasText(type, "No type found; expecting [index]/[type]");

        indexAndType = index + "/" + type;

        // check bulk
        boolean hasPattern = indexAndType.contains("{");
        bulk = (hasPattern ? "/_bulk" : indexAndType + "/_bulk");
        refresh = (hasPattern ? "/_refresh" : index + "/_refresh");
    }

    String bulk() {
        return bulk;
    }

    // https://github.com/elasticsearch/elasticsearch/issues/2726
    String targetShards() {
        return indexAndType + "/_search_shards";
    }

    String mapping() {
        return indexAndType + "/_mapping";
    }

    String indexAndType() {
        return indexAndType;
    }

    String type() {
        return type;
    }

    String index() {
        return index;
    }

    @Override
    public String toString() {
        return indexAndType;
    }

    public String refresh() {
        return refresh;
    }
}