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

package org.elasticsearch.hadoop.util.ecs;

public final class FieldNames {

    private FieldNames() {
        // No instances
    }

    // Fields Used:

    // Base:
    // @timestamp - (date) Current time when event is passed in to handler
    public static final String FIELD_TIMESTAMP = "@timestamp";
    // tags - ([keyword]) User providable field
    public static final String FIELD_TAGS = "tags";
    // labels - (object{keyword}) User providable fields
    public static final String FIELD_LABELS = "labels";
    // message - (text) Concatenated message data. TBD
    public static final String FIELD_MESSAGE= "message";

    // Error:
    public static final String FIELD_ERROR = "error";
    // error.message - (text) Original error message.
    public static final String FIELD_ERROR_MESSAGE = "message";
    // error.code - (keyword) (TBD) Can this be the exception type?
    public static final String FIELD_ERROR_CODE = "code";

    // Event:
    public static final String FIELD_EVENT = "event";
    // event.category - (keyword) TBD should this just be "error"?
    public static final String FIELD_EVENT_CATEGORY = "category";
    // event.type - (keyword) type of error handler
    public static final String FIELD_EVENT_TYPE = "type";
    // event.module - (keyword) does this make sense?
    public static final String FIELD_EVENT_MODULE = "module";
    // event.raw - (keyword) This might end up being the stringafied original cause (json data/bulk entry/record info)
    public static final String FIELD_EVENT_RAW = "raw";
    // event.version - (keyword) version of ECS used.
    public static final String FIELD_EVENT_VERSION = "version";

    // Host:
    public static final String FIELD_HOST = "host";
    // host.name - (keyword) Can we get this easily?
    public static final String FIELD_HOST_NAME = "name";
    // host.ip - (ip) Could do a reverse lookup?
    public static final String FIELD_HOST_IP = "ip";
    // host.architecture - (keyword) Get this from Java (x86_64)
    public static final String FIELD_HOST_ARCHITECTURE = "architecture";

    // Host OS:
    public static final String FIELD_HOST_OS = "os";
    // host.os.name - (keyword) Get from Java. (Mac OS X)
    public static final String FIELD_HOST_OS_NAME = "name";
    // host.os.version - (keyword) Get from java (10.12.6)
    public static final String FIELD_HOST_OS_VERSION = "version";

    // host.timezone.offset.sec - (long) default timezone
    public static final String FIELD_HOST_TIMEZONE = "timezone";
    public static final String FIELD_HOST_TIMEZONE_OFFSET = "offset";
    public static final String FIELD_HOST_TIMEZONE_OFFSET_SEC = "sec";
}
