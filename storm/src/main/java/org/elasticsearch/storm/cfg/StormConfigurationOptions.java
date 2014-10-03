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
package org.elasticsearch.storm.cfg;


public interface StormConfigurationOptions {

    String ES_STORM_BOLT_TICK_TUPLE_FLUSH = "es.storm.bolt.tick.tuple.flush";
    String ES_STORM_BOLT_TICK_TUPLE_FLUSH_DEFAULT = "true";

    String ES_STORM_BOLT_ACK = "es.storm.bolt.write.ack";
    String ES_STORM_BOLT_ACK_DEFAULT = "false";

    String ES_STORM_BOLT_FLUSH_ENTRIES_SIZE = "es.storm.bolt.flush.entries.size";

    String ES_STORM_SPOUT_RELIABLE = "es.storm.spout.reliable";
    String ES_STORM_SPOUT_RELIABLE_DEFAULT = "false";

    String ES_STORM_SPOUT_RELIABLE_QUEUE_SIZE = "es.storm.spout.reliable.queue.size";
    String ES_STORM_SPOUT_RELIABLE_QUEUE_SIZE_DEFAULT = "0";

    String ES_STORM_SPOUT_RELIABLE_RETRIES_PER_TUPLE = "es.storm.spout.reliable.retries.per.tuple";
    String ES_STORM_SPOUT_RELIABLE_RETRIES_PER_TUPLE_DEFAULT = "5";

    String ES_STORM_SPOUT_RELIABLE_TUPLE_FAILURE_HANDLE = "es.storm.spout.reliable.handle.tuple.failure";
    String ES_STORM_SPOUT_RELIABLE_TUPLE_FAILURE_HANDLE_DEFAULT = "abort";

    String ES_STORM_SPOUT_FIELDS = "es.storm.spout.fields";
    String ES_STORM_SPOUT_FIELDS_DEFAULT = "";
}
