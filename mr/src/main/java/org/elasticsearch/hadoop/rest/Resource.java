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

import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.StringUtils;

import static org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_OPERATION_UPDATE;
import static org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_OPERATION_UPSERT;


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

        // 1) Resource must not be null
        String errorMessage = "invalid resource given; expecting [index]/[type] - received ";
        Assert.hasText(resource, errorMessage + resource);

        // 2) Resource may contain a query, so retrieve it and complain if it's already set
        if (resource.contains("?") || resource.contains("&")) {
            if (StringUtils.hasText(settings.getQuery())) {
                throw new EsHadoopIllegalArgumentException(String.format(
                        "Cannot specify a query in the target index AND through %s", ConfigurationOptions.ES_QUERY));
            }

            int index = resource.indexOf("?");
            if (index > 0) {
                String query = resource.substring(index);

                // clean resource
                resource = resource.substring(0, index);
                index = resource.lastIndexOf("/");
                resource = (index > 0 ? resource.substring(0, index) : resource);

                settings.setProperty(ConfigurationOptions.ES_RESOURCE, resource);
                settings.setQuery(query);
            }
        }

        String res = StringUtils.sanitizeResource(resource);

        // 3) Resource must contain an index, but may not necessarily contain a type
        int slash = res.indexOf("/");
        if (slash < 0) {
            // No type is fine for reads since we assume reading from all mappings found for the given index pattern.
            // No type is fatal for writes since we need a type to hand the bulk api.
            if (read == false) {
                throw new EsHadoopIllegalArgumentException(String.format(
                        "No type found; Types are required when writing. Expected [index]/[type], but got [%s]",
                        resource
                ));
            }
            index = res;
            type = StringUtils.EMPTY;
        }
        else {
            index = res.substring(0, slash);
            type = res.substring(slash + 1);

            Assert.hasText(type, "No type found; expecting [index]/[type]");
        }
        Assert.hasText(index, "No index found; expecting [index]/[type]");
        Assert.isTrue(!StringUtils.hasWhitespace(index) && !StringUtils.hasWhitespace(type), "Index and type should not contain whitespaces");
        
        indexAndType = index + "/" + type;

        // 4) Render the other endpoints
        String bulkEndpoint = "/_bulk";

        String ingestPipeline = settings.getIngestPipeline();
        if (StringUtils.hasText(ingestPipeline)) {
            Assert.isTrue(!StringUtils.hasWhitespace(ingestPipeline), "Ingest Pipeline name should not contain whitespaces");
            Assert.isTrue(!(ES_OPERATION_UPDATE.equals(settings.getOperation()) || ES_OPERATION_UPSERT.equals(settings.getOperation())), "Cannot specify an ingest pipeline when doing updates or upserts");
            bulkEndpoint = bulkEndpoint + "?pipeline=" + ingestPipeline;
        }

        // check bulk
        bulk = (indexAndType.contains("{") ? bulkEndpoint : indexAndType + bulkEndpoint);
        refresh = (index.contains("{") ? "/_refresh" : index + "/_refresh");
    }

    String bulk() {
        return bulk;
    }

    String mapping() {
        return indexAndType + "/_mapping";
    }

    String aliases() {
        return index + "/_aliases";
    }

    public String index() {
        return index;
    }

    public String type() {
        return type;
    }

    @Override
    public String toString() {
        return indexAndType;
    }

    public String refresh() {
        return refresh;
    }
}