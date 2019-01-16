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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.EsMajorVersion;
import org.elasticsearch.hadoop.util.StringUtils;

import static org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_OPERATION_UPDATE;
import static org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_OPERATION_UPSERT;


/**
 * ElasticSearch Rest Resource - index and type.
 */
public class Resource {

    private static final Log LOG = LogFactory.getLog(Resource.class);

    public static final String UNDERSCORE_DOC = "_doc";

    private final String index;
    private final boolean typed;
    private final String type;
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

        // 3) Resource must contain an index, but may not necessarily contain a type.
        // This is dependent on the version of ES we are talking with.
        int slash = res.indexOf("/");
        boolean typeExists = slash >= 0;

        EsMajorVersion esMajorVersion = settings.getInternalVersionOrThrow();
        if (esMajorVersion.after(EsMajorVersion.V_7_X)) {
            // Types can no longer the specified at all! Index names only!
            if (typeExists) {
                throw new EsHadoopIllegalArgumentException(String.format(
                        "Detected type name in resource [%s]. Remove type name to continue.",
                        resource
                ));
            }
        }
        if (esMajorVersion.onOrBefore(EsMajorVersion.V_7_X)) {
            // Type can be specified, but a warning will be returned. An ES 7.X cluster will accept types if include_type_name is true,
            // which we will set in the case of a type existing.
            // This is onOrBefore because we want to print the deprecation log no matter what version of ES they're running on.
            if (typeExists) {
                LOG.warn(String.format(
                        "Detected type name in resource [%s]. Type names are deprecated and will be removed in a later release.",
                        resource
                ));
            }
        }
        if (esMajorVersion.onOrBefore(EsMajorVersion.V_6_X)) {
            // Type is required for writing via the bulk API, but not for reading. No type on a read resource means to read all types.
            // This is important even if we're on a 6.x cluster that enforces a single type per index. 6.x STILL supports opening old 5.x
            // indices in order to ease the upgrade process!!!!
            if (!read && !typeExists) {
                throw new EsHadoopIllegalArgumentException(String.format(
                        "No type found; Types are required when writing in ES versions 6 and below. Expected [index]/[type], but got [%s]",
                        resource
                ));
            }
        }

        // Parse out the type if it exists and is valid.
        if (typeExists) {
            index = res.substring(0, slash);
            type = res.substring(slash + 1);
            typed = true;
            Assert.hasText(type, "No type found; expecting [index]/[type]");
        } else {
            index = res;
            type = UNDERSCORE_DOC;
            typed = false;
        }
        Assert.hasText(index, "No index found; expecting [index]/[type]");
        Assert.isTrue(!StringUtils.hasWhitespace(index) && !StringUtils.hasWhitespace(type), "Index/type should not contain whitespaces");

        // 4) Render the other endpoints
        String bulkEndpoint = "/_bulk";

        String ingestPipeline = settings.getIngestPipeline();
        if (StringUtils.hasText(ingestPipeline)) {
            Assert.isTrue(!StringUtils.hasWhitespace(ingestPipeline), "Ingest Pipeline name should not contain whitespaces");
            Assert.isTrue(!(ES_OPERATION_UPDATE.equals(settings.getOperation()) || ES_OPERATION_UPSERT.equals(settings.getOperation())), "Cannot specify an ingest pipeline when doing updates or upserts");
            bulkEndpoint = bulkEndpoint + "?pipeline=" + ingestPipeline;
        }

        // check bulk
        if (index.contains("{") || (typed && type.contains("{"))) {
            bulk = bulkEndpoint;
        } else if (typed){
            bulk = index + "/" + type + bulkEndpoint;
        } else {
            bulk = index + bulkEndpoint;
        }
        refresh = (index.contains("{") ? "/_refresh" : index + "/_refresh");
    }

    String bulk() {
        return bulk;
    }

    String mapping() {
        if (typed) {
            return index + "/_mapping/" + type;
        } else {
            return index + "/_mapping";
        }
    }

    String aliases() {
        return index + "/_aliases";
    }

    public String index() {
        return index;
    }

    public boolean isTyped() {
        return typed;
    }

    public String type() {
        return type;
    }

    @Override
    public String toString() {
        if (typed) {
            return index + "/" + type;
        } else {
            return index;
        }
    }

    public String refresh() {
        return refresh;
    }
}