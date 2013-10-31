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

import java.util.Map;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.serialization.IdExtractor;
import org.elasticsearch.hadoop.serialization.SettingsAware;
import org.elasticsearch.hadoop.util.Assert;

public class HiveIdExtractor implements IdExtractor, SettingsAware {

    private String id;

    @Override
    public String id(Object target) {
        if (target instanceof HiveType) {
            HiveType type = (HiveType) target;
            ObjectInspector inspector = type.getObjectInspector();
            if (inspector instanceof StructObjectInspector) {
                StructObjectInspector soi = (StructObjectInspector) inspector;
                StructField field = soi.getStructFieldRef(id);
                ObjectInspector foi = field.getFieldObjectInspector();
                Assert.isTrue(foi.getCategory() == ObjectInspector.Category.PRIMITIVE,
                        String.format("Id field [%s] needs to be a primitive; found [%s]", id, foi.getTypeName()));

                // expecting a writeable - simply do a toString
                return soi.getStructFieldData(type.getObject(), field).toString();
            }
        }

        return null;
    }

    @Override
    public void setSettings(Settings settings) {
        Map<String, String> columnNames = HiveUtils.columnMap(settings);
        // replace column name with _colX (which is what Hive uses during serialization)
        id = columnNames.get(settings.getMappingId().trim().toLowerCase());
    }
}
