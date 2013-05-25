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

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.serde2.lazy.LazyArray;
import org.apache.hadoop.hive.serde2.lazy.LazyMap;
import org.apache.hadoop.hive.serde2.lazy.LazyPrimitive;
import org.apache.hadoop.hive.serde2.lazy.LazyStruct;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.apache.hadoop.io.Writable;
import org.elasticsearch.hadoop.serialization.Generator;
import org.elasticsearch.hadoop.serialization.ValueWriter;

/**
 * Main value writer for hive. However since Hive expects a Writable type to be passed to the record reader,
 * the raw JSON data needs to be wrapped (and unwrapped by {@link HiveBytesValueWriter}.
 */
public class HiveValueWriter implements ValueWriter<HiveType> {

    private final boolean writeUnknownTypes;
    private final ValueWriter<Writable> writableWriter;

    public HiveValueWriter() {
        this(false);
    }

    public HiveValueWriter(boolean writeUnknownTypes) {
        this.writeUnknownTypes = writeUnknownTypes;
        writableWriter = new HiveWritableValueWriter(writeUnknownTypes);
    }

    @Override
    public boolean write(HiveType type, Generator generator) {
        boolean result = write(type.getObject(), type.getInfo(), generator);
        return result;
    }

    private boolean write(Object data, TypeInfo type, Generator generator) {
        if (data == null) {
            generator.writeNull();
            return true;
        }

        switch (type.getCategory()) {
        case PRIMITIVE:
            Writable writable = null;

            // handle lazy objects differently as the Lazy ObjectInspector breaks the generic contract for #getPrimitive
            if (data instanceof LazyPrimitive) {
                writable = ((LazyPrimitive<?, ?>) data).getWritableObject();
            }
            else {
                // unwrap writable
                writable = (Writable) ((PrimitiveObjectInspector) TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(type)).getPrimitiveWritableObject(data);
            }
            if (!writableWriter.write(writable, generator)) {
                return false;
            }
            break;

        case LIST: // or ARRAY
            ListTypeInfo listType = (ListTypeInfo) type;
            TypeInfo listElementType = listType.getListElementTypeInfo();

            generator.writeBeginArray();
            if (data instanceof LazyArray || data instanceof List) {
                List<?> list = (data instanceof LazyArray ? ((LazyArray) data).getList() : (List<?>) data);
                for (Object object : list) {
                    if (!write(object, listElementType, generator)) {
                        return false;
                    }
                }
            }
            else {
                if (data.getClass().isArray()) {
                    for (Object object : (Object[]) data) {
                        if (!write(object, listElementType, generator)) {
                            return false;
                        }
                    }
                }
                else {
                    handleUnknown(data, type, generator);
                }
            }
            generator.writeEndArray();

            break;

        case MAP:
            MapTypeInfo mapType = (MapTypeInfo) type;
            Map<?, ?> mapContent = null;

            if (data instanceof LazyMap) {
                mapContent = ((LazyMap) data).getMap();
            }
            else {
                mapContent = (Map<?, ?>) data;
            }

            generator.writeBeginObject();
            for (Map.Entry<?, ?> entry : mapContent.entrySet()) {
                //write(entry.getKey(), mapType.getMapKeyTypeInfo(), generator);
                // TODO: handle non-strings
                generator.writeFieldName(entry.getKey().toString());

                if (!write(entry.getValue(), mapType.getMapValueTypeInfo(), generator)) {
                    return false;
                }
            }
            generator.writeEndObject();

            break;

        case STRUCT:
            StructTypeInfo structType = (StructTypeInfo) type;
            List<TypeInfo> info = structType.getAllStructFieldTypeInfos();
            List<String> names = structType.getAllStructFieldNames();

            generator.writeBeginObject();
            // handle the list
            if (data instanceof LazyStruct || data instanceof List) {
                List<?> content = (data instanceof LazyStruct ? ((LazyStruct) data).getFieldsAsList() : (List<?>) data);
                for (int structIndex = 0; structIndex < info.size(); structIndex++) {
                    generator.writeFieldName(names.get(structIndex));
                    if (!write(content.get(structIndex), info.get(structIndex), generator)) {
                        return false;
                    }
                }
            }
            // fall-back to array
            else {
                Object[] content = (Object[]) data;
                for (int structIndex = 0; structIndex < info.size(); structIndex++) {
                    generator.writeFieldName(names.get(structIndex));
                    if (!write(content[structIndex], info.get(structIndex), generator)) {
                        return false;
                    }
                }
            }
            generator.writeEndObject();
            break;

        case UNION:
            UnionTypeInfo unionType = (UnionTypeInfo) type;
            throw new UnsupportedOperationException("union not yet supported");//break;


        default:
            if (writeUnknownTypes) {
                return handleUnknown(data, type, generator);
            }
            return false;
        }

        return true;
    }

    protected boolean handleUnknown(Object value, TypeInfo info, Generator generator) {
        return false;
    }
}