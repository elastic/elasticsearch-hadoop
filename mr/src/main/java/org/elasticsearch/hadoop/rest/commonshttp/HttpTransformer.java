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

import org.apache.commons.httpclient.HttpMethod;

/**
 * Classes that implement this interface are low-level HTTP-specific transformers
 * applying arbitrary transformations to any aspect of an ElasticSearch HTTP requests.
 * This includes adding dynamic HTTP headers and request signing (such as AWS SigV4 signing).
 *
 * Instances of this class are created using factory pattern, by implementing
 * {@link HttpTransformerFactory}. For high network/http performance it is recommended
 * to fully initialize the HTTPTransformer instances with all the data that they
 * might need to process requests/responses. Performance of the transformation
 * could be a bottleneck in overall Elasticsearch communication if it requires
 * long time to process input.
 *
 * The transformations are applied to http requests OR responses (never both)
 * immediately before sending the requests to ElasticSearch or immediately after
 * receiving a response. (see {@link HttpTransformerFactory#getExecutionType()}
 * for more info.
 *
 * @see HttpTransformerFactory
 */
public interface HttpTransformer {

    /**
     * Performs optional modification of HttpMethod instances immediately before
     * sending the request to ElasticSearch or immediately after receiving the reply.
     *
     * Method can analyze and change all the attributes of the request, including
     * destination, url, HTTP verb, headers and body.
     *
     * Returned HttpMethod may or may not be the same instance as an input.
     *
     * @param httpMethod HttpMethod instance to transform, guaranteed to be non-null
     * @return transformed HTTPMethod instance, required to be non-null
     */
    HttpMethod transform(HttpMethod httpMethod);
}
