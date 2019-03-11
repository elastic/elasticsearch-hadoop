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

package org.elasticsearch.hadoop.rest.bulk;

import java.io.IOException;
import java.io.InputStream;

import org.elasticsearch.hadoop.rest.Resource;
import org.elasticsearch.hadoop.rest.RestClient;

/**
 * Test class for producing bulk endpoint output data programatically for different versions.
 */
public interface BulkOutputGenerator {

    public BulkOutputGenerator setInfo(Resource resource, long took);

    public BulkOutputGenerator addSuccess(String operation, int status);

    public BulkOutputGenerator addFailure(String operation, int status, String type, String errorMessage);

    public BulkOutputGenerator addRejection(String operation);

    public RestClient.BulkActionResponse generate() throws IOException;

}
