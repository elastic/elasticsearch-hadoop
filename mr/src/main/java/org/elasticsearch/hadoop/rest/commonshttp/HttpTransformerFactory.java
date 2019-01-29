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
package org.elasticsearch.hadoop.rest.commonshttp;

import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.security.SecureSettings;

/**
 * Implementations of this class are instantiated by ElasticSearch internal HTTP
 * transport implementation to create instances of {@link HttpTransformer} used to
 * transform input or output of an HTTP request submitted to ElasticSearch clusters.
 *
 * The full names of the classes implementing this interface are supposed to be supplied
 * into the 'es.net.http.transformer.factories' configuration property. If multiple classes
 * are used, separate them with comma.
 *
 * All implementing classes MUST have a default no-arg public constructor.
 *
 * @see HttpTransformer
 */
public interface HttpTransformerFactory {
    /**
     * Specifies when the HttpTransformer needs to be applied: before sending the request
     * or after receiving a response.
     */
    enum HttpTransformerExecutionType { BEFORE, AFTER }

    /**
     * Defines when the HttpTransformer produced by the {@link #getHttpTransformer(Settings, SecureSettings, String)}
     * method should be executed.
     *
     * @return BEFORE or AFTER depending on the logic of the tranformer, must be non-null
     */
    HttpTransformerExecutionType getExecutionType();

    /**
     * Creates an instance of {@link HttpTransformer} that will be applied to every request or response
     * that is send to ElasticSearch cluster.
     *
     * @see HttpTransformer
     *
     * @param settings hadoop-specific ElasticSearch settings object
     * @param secureSettings secure setings, wrapped into the KeyStore
     * @param hostInfo execution host inforrmation
     * @return instance of an {@link HttpTransformer}
     */
    HttpTransformer getHttpTransformer(Settings settings, SecureSettings secureSettings, String hostInfo);
}
