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
package org.elasticsearch.hadoop.mr;

/**
 * Class providing the various Configuration parameters used by the ElasticSearch Map/Reduce Input/Output formats.
 */
public interface ESConfigConstants {

    public static final String ES_HOST = "es.host";
    public static final String ES_PORT = "es.port";
    public static final String ES_LOCATION = "es.location";

    /**
     * For internal use - do not define.
     */
    public static final String ES_INDEX = "es.mr.index";
    public static final String ES_QUERY = "es.mr.query";

    public static final String ES_ADDRESS = "es.address";
}
