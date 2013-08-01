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

import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.elasticsearch.hadoop.rest.EsType;
import org.elasticsearch.hadoop.serialization.Generator;
import org.elasticsearch.hadoop.serialization.ValueWriter;
import org.elasticsearch.hadoop.util.StringUtils;

class HiveSchemaWriter implements ValueWriter<StructObjectInspector> {

    private final String typeName;

    public HiveSchemaWriter(String type) {
        this.typeName = type;
    }

    @Override
    public boolean write(StructObjectInspector object, Generator generator) {
        generator.writeBeginObject();
        generator.writeFieldName(typeName);
        generator.writeBeginObject();
        generator.writeFieldName("properties");
        generator.writeBeginObject();

        for (StructField field : object.getAllStructFieldRefs()) {
            if (!write(field.getFieldObjectInspector(), generator, field.getFieldName(), true)) {
                return false;
            }
        }

        generator.writeEndObject();
        generator.writeEndObject();
        generator.writeEndObject();
        return true;
    }

    private boolean write(ObjectInspector inspector, Generator generator, String fieldName, boolean notAnArray) {
        if (StringUtils.hasText(fieldName)) {
            generator.writeFieldName(fieldName);
        }

        if (notAnArray) {
            generator.writeBeginObject();
        }

        EsType esType = EsType.STRING;

        switch (inspector.getCategory()) {
        case MAP:
            MapObjectInspector moi = (MapObjectInspector) inspector;

            ObjectInspector keyInspector = moi.getMapKeyObjectInspector();
            if (keyInspector.getCategory() != Category.PRIMITIVE || ((PrimitiveObjectInspector) keyInspector).getPrimitiveCategory() != PrimitiveCategory.STRING) {
                throw new IllegalArgumentException("Only String is supported as key type for maps; " + moi.getTypeName());
            }

            generator.writeFieldName("properties");
            generator.writeBeginObject();

            generator.writeEndObject();
            esType = null;
            throw new UnsupportedOperationException("Map not supported yet");
            //break;

        case STRUCT:
            StructObjectInspector soi = (StructObjectInspector) inspector;

            generator.writeBeginObject();
            generator.writeFieldName("properties");
            generator.writeBeginObject();

            for (StructField field : soi.getAllStructFieldRefs()) {
                if (!write(field.getFieldObjectInspector(), generator, field.getFieldName(), true)) {
                    return false;
                }
            }

            generator.writeEndObject();
            generator.writeEndObject();

            esType = null;
            break;

        case LIST:
            ListObjectInspector loi = (ListObjectInspector) inspector;
            ObjectInspector listElementObjectInspector = loi.getListElementObjectInspector();

            // skip the name since it's an array
            if (!write(listElementObjectInspector, generator, null, false)) {
                return false;
            }
            esType = null;
            break;

        case PRIMITIVE:
            PrimitiveObjectInspector poi = (PrimitiveObjectInspector) inspector;
            switch (poi.getPrimitiveCategory()) {
            case BOOLEAN:
                esType = EsType.BOOLEAN;
                break;
            case BYTE:
                esType = EsType.BYTE;
                break;
            case SHORT:
                esType = EsType.SHORT;
                break;
            case INT:
                esType = EsType.INTEGER;
                break;
            case LONG:
                esType = EsType.LONG;
                break;
            case FLOAT:
                esType = EsType.FLOAT;
                break;
            case DOUBLE:
                esType = EsType.DOUBLE;
                break;
            case TIMESTAMP:
                esType = EsType.DATE;
                break;
            case BINARY:
                esType = EsType.BINARY;
                break;

            // to be safe, save it as a String
            case DECIMAL:
            // treat unknown as strings
            case UNKNOWN:
            case VOID:
            case STRING:
            default:
                esType = EsType.STRING;
            }
            break;
        default:

        }

        if (esType != null) {
            generator.writeFieldName("type");
            generator.writeString(esType.value());
        }

        if (notAnArray) {
            generator.writeEndObject();
        }
        return true;
    }
}