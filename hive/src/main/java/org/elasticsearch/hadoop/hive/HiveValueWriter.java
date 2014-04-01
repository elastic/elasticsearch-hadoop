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
import java.util.Map;

import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Writable;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.serialization.Generator;
import org.elasticsearch.hadoop.serialization.SettingsAware;
import org.elasticsearch.hadoop.serialization.builder.ValueWriter;
import org.elasticsearch.hadoop.util.FieldAlias;

/**
 * Main value writer for hive. However since Hive expects a Writable type to be passed to the record reader,
 * the raw JSON data needs to be wrapped (and unwrapped by {@link HiveBytesArrayWritable}).
 */
public class HiveValueWriter implements SettingsAware, ValueWriter<HiveType> {

    private final boolean writeUnknownTypes;
    private final ValueWriter<Writable> writableWriter;
    private FieldAlias alias;

    public HiveValueWriter() {
        this.writeUnknownTypes = false;
        this.writableWriter = new HiveWritableValueWriter(false);
        this.alias = new FieldAlias();
    }

    @Override
    public boolean write(HiveType type, Generator generator) {
        boolean result = write(type.getObject(), type.getObjectInspector(), generator);
        return result;
    }

    private boolean write(Object data, ObjectInspector oi, Generator generator) {
        if (data == null) {
            generator.writeNull();
            return true;
        }

        switch (oi.getCategory()) {
        case PRIMITIVE:
            Writable writable = (Writable) ((PrimitiveObjectInspector) oi).getPrimitiveWritableObject(data);

            if (!writableWriter.write(writable, generator)) {
                return false;
            }
            break;

        case LIST: // or ARRAY
            ListObjectInspector loi = (ListObjectInspector) oi;
            generator.writeBeginArray();

            for (int i = 0; i < loi.getListLength(data); i++) {
                if (!write(loi.getListElement(data, i), loi.getListElementObjectInspector(), generator)) {
                    return false;
                }
            }
            generator.writeEndArray();

            break;

        case MAP:
            MapObjectInspector moi = (MapObjectInspector) oi;

            generator.writeBeginObject();
            for (Map.Entry<?, ?> entry : moi.getMap(data).entrySet()) {
                //write(entry.getKey(), mapType.getMapKeyTypeInfo(), generator);
                // TODO: handle non-strings
                generator.writeFieldName(alias.toES(entry.getKey().toString()));
                if (!write(entry.getValue(), moi.getMapValueObjectInspector(), generator)) {
                    return false;
                }
            }
            generator.writeEndObject();

            break;

        case STRUCT:
            StructObjectInspector soi = (StructObjectInspector) oi;

            List<? extends StructField> refs = soi.getAllStructFieldRefs();

            generator.writeBeginObject();
            for (StructField structField : refs) {
                generator.writeFieldName(alias.toES(structField.getFieldName()));
                if (!write(soi.getStructFieldData(data, structField), structField.getFieldObjectInspector(), generator)) {
                    return false;
                }
            }
            generator.writeEndObject();
            break;

        case UNION:
            //UnionObjectInspector uoi = (UnionObjectInspector) oi;
            throw new UnsupportedOperationException("union not yet supported");//break;


        default:
            if (writeUnknownTypes) {
                return handleUnknown(data, oi, generator);
            }
            return false;
        }

        return true;
    }


    protected boolean handleUnknown(Object value, ObjectInspector oi, Generator generator) {
        return false;
    }

    @Override
    public void setSettings(Settings settings) {
        alias = HiveUtils.alias(settings);
    }
}