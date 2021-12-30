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
package org.elasticsearch.hadoop.hive;

/**
 * Holder class for various Hive configuration options. Exists mainly since the Hive classes have been refactored/renamed between releases which are not backwards compatible.
 */
interface HiveConstants {

    String COLUMNS = "columns";
    String COLUMNS_TYPES = "columns.types";
    String UNNAMED_COLUMN_PREFIX="_col";

    String DECIMAL_WRITABLE = "org.apache.hadoop.hive.serde2.io.HiveDecimalWritable";
    String DATE_WRITABLE = "org.apache.hadoop.hive.serde2.io.DateWritable";
    String VARCHAR_WRITABLE = "org.apache.hadoop.hive.serde2.io.HiveVarcharWritable";
    String CHAR_WRITABLE = "org.apache.hadoop.hive.serde2.io.HiveCharWritable";
    String TIMESTAMP_WRITABLE_V2 = "org.apache.hadoop.hive.serde2.io.TimestampWritableV2";
    String DATE_WRITABLE_V2 = "org.apache.hadoop.hive.serde2.io.DateWritableV2";
    String TABLE_LOCATION = "location";

    String MAPPING_NAMES = "es.mapping.names";
    String COLUMN_COMMENTS = "columns.comments";

    String INPUT_TBL_PROPERTIES = "es.internal.hive.input.tbl.properties";
    String OUTPUT_TBL_PROPERTIES = "es.internal.hive.output.tbl.properties";
    String[] VIRTUAL_COLUMNS = new String[] { "INPUT__FILE__NAME", "BLOCK__OFFSET__INSIDE__FILE",
            "ROW__OFFSET__INSIDE__BLOCK", "RAW__DATA__SIZE", "GROUPING__ID" };
}
