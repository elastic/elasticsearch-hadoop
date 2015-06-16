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

import java.util.Locale;
import java.util.Map;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.NullWritable;
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.serialization.field.ConstantFieldExtractor;
import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.StringUtils;

public class HiveFieldExtractor extends ConstantFieldExtractor {

    private String fieldName;

    @Override
    protected Object extractField(Object target) {
        if (target instanceof HiveType) {
            HiveType type = (HiveType) target;
            ObjectInspector inspector = type.getObjectInspector();
            if (inspector instanceof StructObjectInspector) {
                StructObjectInspector soi = (StructObjectInspector) inspector;
                StructField field = soi.getStructFieldRef(fieldName);
                ObjectInspector foi = field.getFieldObjectInspector();
                Assert.isTrue(foi.getCategory() == ObjectInspector.Category.PRIMITIVE,
                        String.format("Field [%s] needs to be a primitive; found [%s]", fieldName, foi.getTypeName()));

                // expecting a writeable - simply do a toString
                Object data = soi.getStructFieldData(type.getObject(), field);
                if (data == null || data instanceof NullWritable) {
                    return StringUtils.EMPTY;
                }
                return data.toString();
            }
        }

        return null;
    }

    @Override
    public void processField(Settings settings, String fl) {
        Map<String, String> columnNames = HiveUtils.columnMap(settings);
        // replace column name with _colX (which is what Hive uses during serialization)
        fieldName = columnNames.get(fl.toLowerCase(Locale.ROOT));

        if (!settings.getInputAsJson() && !StringUtils.hasText(fieldName)) {
            throw new EsHadoopIllegalArgumentException(
                    String.format(
                            "Cannot find field [%s] in mapping %s ; maybe a value was specified without '<','>' or there is a typo?",
                            fl, columnNames.keySet()));
        }
    }
}
