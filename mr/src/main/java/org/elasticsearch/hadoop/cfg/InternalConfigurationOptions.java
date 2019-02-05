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
package org.elasticsearch.hadoop.cfg;

/**
 * Property names for internal framework use. They will show up inside the Hadoop configuration or Cascading properties (which act as a distributing support) but not in the API.
 */
public interface InternalConfigurationOptions extends ConfigurationOptions {

    String INTERNAL_ES_TARGET_FIELDS = "es.internal.mr.target.fields";
    // discovered node
    String INTERNAL_ES_DISCOVERED_NODES = "es.internal.discovered.nodes";
    // pinned node
    String INTERNAL_ES_PINNED_NODE = "es.internal.pinned.node";

    String INTERNAL_ES_QUERY_FILTERS = "es.internal.query.filters";

    String INTERNAL_ES_VERSION = "es.internal.es.version";
    String INTERNAL_ES_CLUSTER_NAME = "es.internal.es.cluster.name";
    String INTERNAL_ES_CLUSTER_UUID = "es.internal.es.cluster.uuid";
    // used for isolating connection pools of multiple spark streaming jobs in the same app.
    String INTERNAL_TRANSPORT_POOLING_KEY = "es.internal.transport.pooling.key";

    // don't fetch _source field during scroll queries
    String INTERNAL_ES_EXCLUDE_SOURCE = "es.internal.exclude.source";
    String INTERNAL_ES_EXCLUDE_SOURCE_DEFAULT = "false";
}
