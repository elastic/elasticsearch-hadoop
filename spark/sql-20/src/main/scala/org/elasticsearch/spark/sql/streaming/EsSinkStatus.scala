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

package org.elasticsearch.spark.sql.streaming

/**
 * An envelope of metadata that is stored in the commit log. Mostly contains statistics about
 * what was written in the given batch.
 *
 * @param taskId of the task that wrote this data
 * @param execTimeMillis when the data was written
 * @param resource which resource the data was written to, patterns included
 * @param records number of records written to resource
 */
case class EsSinkStatus(taskId: Int, execTimeMillis: Long, resource: String, records: Long)
