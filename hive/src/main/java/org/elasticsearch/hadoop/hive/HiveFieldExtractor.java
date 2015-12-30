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

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.NullWritable;
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.serialization.field.ConstantFieldExtractor;
import org.elasticsearch.hadoop.serialization.field.FieldExtractor;
import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.StringUtils;

public class HiveFieldExtractor extends ConstantFieldExtractor {

    protected List<String> fieldNames;

    @Override
    protected Object extractField(Object target) {
        List<String> flNames = fieldNames;

        for (int i = 0; i < flNames.size(); i++) {
            String fl = flNames.get(i);
            if (target instanceof HiveType) {
                HiveType type = (HiveType) target;
                ObjectInspector inspector = type.getObjectInspector();
                if (inspector instanceof StructObjectInspector) {
                    StructObjectInspector soi = (StructObjectInspector) inspector;
                    StructField field = soi.getStructFieldRef(fl);
                    ObjectInspector foi = field.getFieldObjectInspector();
                    Assert.isTrue(foi.getCategory() == ObjectInspector.Category.PRIMITIVE,
                            String.format("Field [%s] needs to be a primitive; found [%s]", fl, foi.getTypeName()));

                    // expecting a writeable - simply do a toString
                    target = soi.getStructFieldData(type.getObject(), field);
                }
                else {
                    return FieldExtractor.NOT_FOUND;
                }
            }
            else {
                return FieldExtractor.NOT_FOUND;
            }
        }

        if (target == null || target instanceof NullWritable) {
            return StringUtils.EMPTY;
        }
        return target.toString();
    }

    @Override
    public void processField(Settings settings, List<String> fl) {
        Map<String, String> columnNames = HiveUtils.columnMap(settings);
        // replace column name with _colX (which is what Hive uses during serialization)
        fieldNames = new ArrayList<String>(fl.size());
        for (String string : fl) {
            fieldNames.add(columnNames.get(string.toLowerCase(Locale.ROOT)));
        }

        if (!settings.getInputAsJson() && fl.isEmpty()) {
            throw new EsHadoopIllegalArgumentException(
                    String.format(
                            "Cannot find field [%s] in mapping %s ; maybe a value was specified without '<','>' or there is a typo?",
                            fl, columnNames.keySet()));
        }
    }
}
