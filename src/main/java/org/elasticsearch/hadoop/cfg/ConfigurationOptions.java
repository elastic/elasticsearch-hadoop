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
package org.elasticsearch.hadoop.cfg;

/**
 * Class providing the various Configuration parameters used by the ElasticSearch Hadoop integration.
 */
public interface ConfigurationOptions {

    /** ElasticSearch host **/
    static final String ES_HOST = "es.host";
    static final String ES_HOST_DEFAULT = "localhost";

    /** ElasticSearch port **/
    static final String ES_PORT = "es.port";
    static final String ES_PORT_DEFAULT = "9200";

    /** ElasticSearch index or query (so called location) */
    static final String ES_RESOURCE = "es.resource";

    /** ElasticSearch batch size given in bytes */
    static final String ES_BATCH_SIZE_BYTES = "es.batch.size.bytes";
    static final String ES_BATCH_SIZE_BYTES_DEFAULT = "10mb";

    /** ElasticSearch batch size given in entries */
    static final String ES_BATCH_SIZE_ENTRIES = "es.batch.size.entries";
    static final String ES_BATCH_SIZE_ENTRIES_DEFAULT = "0";

    /** Whether to trigger an index refresh after doing batch writing */
    static final String ES_BATCH_WRITE_REFRESH = "es.batch.write.refresh";
    static final String ES_BATCH_WRITE_REFRESH_DEFAULT = "true";

    /** HTTP connection timeout */
    static final String ES_HTTP_TIMEOUT = "es.http.timeout";
    static final String ES_HTTP_TIMEOUT_DEFAULT = "1m";
}
