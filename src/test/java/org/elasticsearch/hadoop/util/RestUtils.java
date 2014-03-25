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

import java.io.IOException;

import org.elasticsearch.hadoop.rest.Request;
import org.elasticsearch.hadoop.rest.Request.Method;
import org.elasticsearch.hadoop.rest.Response;
import org.elasticsearch.hadoop.rest.RestClient;
import org.elasticsearch.hadoop.rest.RestClient.HEALTH;
import org.elasticsearch.hadoop.util.unit.TimeValue;

public class RestUtils {

    public static class ExtendedRestClient extends RestClient {

        public ExtendedRestClient() {
            super(new TestSettings());
        }

        @Override
        public Response execute(Method method, String path, ByteSequence buffer) {
            return super.execute(method, path, buffer);
        }

        public String put(String index, byte[] buffer) throws IOException {
            return IOUtils.asString(execute(Request.Method.POST, index, new BytesArray(buffer)).body());
        }

        public String refresh(String index) throws IOException {
            return IOUtils.asString(execute(Request.Method.POST, index + "/_refresh", null).body());
        }
    }

    public static void putMapping(String index, byte[] content) throws Exception {
        RestClient rc = new ExtendedRestClient();
        BytesArray bs = new BytesArray(content);
        rc.putMapping(index, index + "/_mapping", bs.bytes);
        rc.close();
    }

    public static void putMapping(String index, String location) throws Exception {
        putMapping(index, TestUtils.fromInputStream(RestUtils.class.getClassLoader().getResourceAsStream(location)));
    }


    public static void putData(String index, String location) throws Exception {
        putData(index, TestUtils.fromInputStream(RestUtils.class.getClassLoader().getResourceAsStream(location)));
    }

    public static void putData(String index, byte[] content) throws Exception {
        ExtendedRestClient rc = new ExtendedRestClient();
        rc.put(index, content);
        rc.close();
    }

    public static void waitForYellow(String string) throws Exception {
        ExtendedRestClient rc = new ExtendedRestClient();
        rc.health(string, HEALTH.YELLOW, TimeValue.timeValueSeconds(5));
        rc.close();
    }

    public static void refresh(String string) throws Exception {
        ExtendedRestClient rc = new ExtendedRestClient();
        rc.refresh(string);
        rc.close();
    }

    public static boolean exists(String string) throws Exception {
        ExtendedRestClient rc = new ExtendedRestClient();
        boolean result = rc.exists(string);
        rc.close();
        return result;
    }

}
