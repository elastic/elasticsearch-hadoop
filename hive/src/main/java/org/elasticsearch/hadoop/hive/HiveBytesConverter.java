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

import java.util.List;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.elasticsearch.hadoop.mr.WritableBytesConverter;
import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.BytesArray;

public class HiveBytesConverter extends WritableBytesConverter {

    @Override
    public void convert(Object from, BytesArray to) {

        Assert.isTrue(from instanceof HiveType,
                String.format("Unexpected object type, expecting [%s], given [%s]", HiveType.class, from.getClass()));

        HiveType ht = (HiveType) from;
        ObjectInspector oi = ht.getObjectInspector();

        Assert.isTrue(Category.STRUCT == oi.getCategory(),
                String.format("Unexpected object category, expecting [%s], given [%s]", Category.STRUCT, oi.getTypeName()));

        StructObjectInspector soi = (StructObjectInspector) oi;
        List<? extends StructField> refs = soi.getAllStructFieldRefs();
        Assert.isTrue(refs.size() == 1, "When using JSON input, only one field is expected");

        StructField structField = refs.get(0);
        ObjectInspector foi = structField.getFieldObjectInspector();

        Assert.isTrue(Category.PRIMITIVE == foi.getCategory(),
                String.format("Unexpected object category, expecting [%s], given [%s]", Category.PRIMITIVE, oi.getTypeName()));

        Object writable = ((PrimitiveObjectInspector) foi).getPrimitiveWritableObject(soi.getStructFieldData(ht.getObject(), structField));

        // HiveVarcharWritable - Hive 0.12+
        if (writable != null && HiveConstants.VARCHAR_WRITABLE.equals(writable.getClass().getName())) {
            // TODO: add dedicated optimization
            to.bytes(writable.toString());
            return;
        }

        super.convert(writable, to);
    }
}
