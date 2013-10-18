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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.elasticsearch.hadoop.util.StringUtils;

abstract class HiveUtils {

    // Date type available since Hive 0.12
    static final boolean DATE_WRITABLE_AVAILABLE;

    static {
        ClassLoader cl = (TimestampWritable.class.getClassLoader());
        Class<?> clz = null;
        try {
            clz = cl.loadClass(HiveConstants.DATE_WRITABLE);
        } catch (Exception ex) {
            // ignore
        }

        DATE_WRITABLE_AVAILABLE = (clz != null);
    }

    static StandardStructObjectInspector structObjectInspector(Properties tableProperties) {
        // extract column info - don't use Hive constants as they were renamed in 0.9 breaking compatibility

        // the column names are saved as the given inspector to #serialize doesn't preserves them (maybe because it's an external table)
        // use the class since StructType requires it ...
        List<String> columnNames = StringUtils.tokenize(tableProperties.getProperty(HiveConstants.COLUMNS), ",");
        List<TypeInfo> colTypes = TypeInfoUtils.getTypeInfosFromTypeString(tableProperties.getProperty(HiveConstants.COLUMNS_TYPES));

        // create a standard writable Object Inspector - used later on by serialization/deserialization
        List<ObjectInspector> inspectors = new ArrayList<ObjectInspector>();

        for (TypeInfo typeInfo : colTypes) {
            inspectors.add(TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(typeInfo));
        }

        return ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, inspectors);
    }

    static StructTypeInfo typeInfo(StructObjectInspector inspector) {
        return (StructTypeInfo) TypeInfoUtils.getTypeInfoFromObjectInspector(inspector);
    }

    static FieldAlias alias(Properties tableProperties) {
        List<String> aliases = StringUtils.tokenize(tableProperties.getProperty(HiveConstants.MAPPING_NAMES), ",");

        Map<String, String> aliasMap = new LinkedHashMap<String, String>();

        if (aliases != null) {
            for (String string : aliases) {
                // split alias
                string = string.trim();
                int index = string.indexOf(":");
                if (index > 0) {
                    String key = string.substring(0, index);
                    // save the lower case version as well since Hive does that for top-level keys
                    aliasMap.put(key, string.substring(index + 1));
                    aliasMap.put(key.toLowerCase(), string.substring(index + 1));
                }
            }
        }

        // add default aliases for serialization (_colX -> mapping name)
        List<String> columnNames = StringUtils.tokenize(tableProperties.getProperty(HiveConstants.COLUMNS), ",");
        for (int i = 0; i < columnNames.size(); i++) {
            String colName = columnNames.get(i);
            if (aliases != null) {
                // check alias first
                String alias = aliasMap.get(colName);
                if (alias != null) {
                    colName = alias;
                }
            }
            aliasMap.put(HiveConstants.UNNAMED_COLUMN_PREFIX + i, colName);
        }

        return new FieldAlias(aliasMap);
    }
}