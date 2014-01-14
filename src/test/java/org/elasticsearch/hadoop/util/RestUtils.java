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
package org.elasticsearch.hadoop.util;

import org.elasticsearch.hadoop.rest.Resource;
import org.elasticsearch.hadoop.rest.RestClient;

public class RestUtils {

    public static void putMapping(String index, byte[] content) throws Exception {
        RestClient rc = new RestClient(new TestSettings());
        BytesArray bs = new BytesArray(content);
        rc.putMapping(index, index + "/_mapping", bs.bytes);
        rc.close();
    }

    public static void putMapping(String index, String location) throws Exception {
        putMapping(index, TestUtils.fromInputStream(RestUtils.class.getClassLoader().getResourceAsStream(location)));
    }

    public static void putData(String index, byte[] content) throws Exception {
        RestClient rc = new RestClient(new TestSettings());
        TrackingBytesArray tba = new TrackingBytesArray(new BytesArray(256));
        tba.copyFrom(new BytesArray(content));
        rc.bulk(new Resource(new TestSettings()), tba);
        rc.close();
    }
}
