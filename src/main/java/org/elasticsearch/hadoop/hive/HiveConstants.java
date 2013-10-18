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
package org.elasticsearch.hadoop.hive;

/**
 * Holder class for various Hive configuration options. Exists mainly since the Hive classes have been refactored/renamed between releases which are not backwards compatible.
 */
interface HiveConstants {

    String OUTPUT_COMMITTER = "mapred.output.committer.class";
    String COLUMNS = "columns";
    String COLUMNS_TYPES = "columns.types";
    String UNNAMED_COLUMN_PREFIX="_col";

    String DECIMAL_WRITABLE = "org.apache.hadoop.hive.serde2.io.HiveDecimalWritable";
    String DATE_WRITABLE = "org.apache.hadoop.hive.serde2.io.DateWritable";
    String VARCHAR_WRITABLE = "org.apache.hadoop.hive.serde2.io.HiveVarcharWritable";
    String TABLE_LOCATION = "location";
    String MAPPING_NAMES = "es.mapping.names";
}
