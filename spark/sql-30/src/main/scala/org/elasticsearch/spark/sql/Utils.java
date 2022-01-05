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
package org.elasticsearch.spark.sql;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.serialization.FieldType;
import org.elasticsearch.hadoop.serialization.dto.mapping.Field;
import org.elasticsearch.hadoop.util.unit.Booleans;

abstract class Utils {

    static final Log LOGGER = LogFactory.getLog("org.elasticsearch.spark.sql.DataSource");
            
    // required since type has a special meaning in Scala
    // and thus the method cannot be called
    static FieldType extractType(Field field) {
        return field.type();
    }

    static final String ROW_INFO_ORDER_PROPERTY = "es.internal.spark.sql.row.order";
    static final String ROW_INFO_ARRAY_PROPERTY = "es.internal.spark.sql.row.arrays";
    static final String ROOT_LEVEL_NAME = "_";

    static final String DATA_SOURCE_PUSH_DOWN = "es.internal.spark.sql.pushdown";
    static final String DATA_SOURCE_PUSH_DOWN_STRICT = "es.internal.spark.sql.pushdown.strict";
    // double filtering (run Spark filters) or not
    static final String DATA_SOURCE_KEEP_HANDLED_FILTERS = "es.internal.spark.sql.pushdown.keep.handled.filters";

    // columns selected by Spark SQL query
    static final String DATA_SOURCE_REQUIRED_COLUMNS = "es.internal.spark.sql.required.columns";

    static boolean isPushDown(Settings cfg) {
        return Booleans.parseBoolean(cfg.getProperty(DATA_SOURCE_PUSH_DOWN), true);
    }

    static boolean isPushDownStrict(Settings cfg) {
        return Booleans.parseBoolean(cfg.getProperty(DATA_SOURCE_PUSH_DOWN_STRICT), false);
    }

    static boolean isKeepHandledFilters(Settings cfg) {
        return Booleans.parseBoolean(cfg.getProperty(DATA_SOURCE_KEEP_HANDLED_FILTERS), true) || !isPushDown(cfg);
    }

    static String camelCaseToDotNotation(String string) {
        StringBuilder sb = new StringBuilder();

        char last = 0;
        for (int i = 0; i < string.length(); i++) {
            char c = string.charAt(i);
            if (Character.isUpperCase(c) && Character.isLowerCase(last)) {
                sb.append(".");
            }
            last = c;
            sb.append(Character.toLowerCase(c));
        }

        return sb.toString();
    }
}

