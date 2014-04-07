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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.elasticsearch.hadoop.EsHadoopIllegalStateException;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.rest.InitializationUtils;
import org.elasticsearch.hadoop.util.FieldAlias;
import org.elasticsearch.hadoop.util.ObjectUtils;
import org.elasticsearch.hadoop.util.SettingsUtils;
import org.elasticsearch.hadoop.util.StringUtils;

abstract class HiveUtils {

    // Date type available since Hive 0.12
    static final boolean DATE_WRITABLE_AVAILABLE = ObjectUtils.isClassPresent(HiveConstants.DATE_WRITABLE,
            TimestampWritable.class.getClassLoader());

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

    static Collection<String> columnToAlias(Settings settings) {
        FieldAlias fa = alias(settings);
        List<String> columnNames = StringUtils.tokenize(settings.getProperty(HiveConstants.COLUMNS), ",");
        // eliminate virtual columns
        // we can't use virtual columns since some distro don't have this field...
        //        for (VirtualColumn vc : VirtualColumn.VIRTUAL_COLUMNS) {
        //            columnNames.remove(vc.getName());
        //        }

        for (String vc : HiveConstants.VIRTUAL_COLUMNS) {
            columnNames.remove(vc);
        }

        for (int i = 0; i < columnNames.size(); i++) {
            String original = columnNames.get(i);
            String alias = fa.toES(original);
            if (alias != null) {
                columnNames.set(i, alias);
            }
        }
        return columnNames;
    }

    static FieldAlias alias(Settings settings) {
        Map<String, String> aliasMap = SettingsUtils.aliases(settings.getProperty(HiveConstants.MAPPING_NAMES));

        // add default aliases for serialization (_colX -> mapping name)
        Map<String, String> columnMap = columnMap(settings);

        for (Entry<String, String> entry : columnMap.entrySet()) {
            String columnName = entry.getKey();
            String columnIndex = entry.getValue();

            if (!aliasMap.isEmpty()) {
                String alias = aliasMap.get(columnName);
                if (alias != null) {
                    columnName = alias;
                }
            }

            aliasMap.put(columnIndex, columnName);
        }

        return new FieldAlias(aliasMap);
    }

    static Map<String, String> columnMap(Settings settings) {
        return columnMap(settings.getProperty(HiveConstants.COLUMNS));
    }

    // returns a map of {<column-name>:_colX}
    private static Map<String, String> columnMap(String columnString) {
        // add default aliases for serialization (mapping name -> _colX)
        List<String> columnNames = StringUtils.tokenize(columnString, ",");
        if (columnNames.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, String> columns = new LinkedHashMap<String, String>();
        for (int i = 0; i < columnNames.size(); i++) {
            columns.put(columnNames.get(i), HiveConstants.UNNAMED_COLUMN_PREFIX + i);
        }
        return columns;
    }

    static void init(Settings settings, Log log) {
        InitializationUtils.checkIdForOperation(settings);
        InitializationUtils.setFieldExtractorIfNotSet(settings, HiveFieldExtractor.class, log);
        try {
            InitializationUtils.discoverEsVersion(settings, log);
        } catch (IOException ex) {
            throw new EsHadoopIllegalStateException("Cannot discover Elasticsearch version", ex);
        }
    }
}