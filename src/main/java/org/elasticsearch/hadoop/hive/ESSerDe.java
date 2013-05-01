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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.lazy.LazyArray;
import org.apache.hadoop.hive.serde2.lazy.LazyMap;
import org.apache.hadoop.hive.serde2.lazy.LazyPrimitive;
import org.apache.hadoop.hive.serde2.lazy.LazyStruct;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class ESSerDe implements SerDe {

    private Configuration conf;
    private StructObjectInspector inspector;
    private ArrayList<String> columnNames;

    @Override
    public void initialize(Configuration conf, Properties tbl) throws SerDeException {
        this.conf = conf;

        // extract column info - don't use Hive constants as they were renamed in 0.9 breaking compatibility

        // the column names are saved as the given inspector to #serialize doesn't preserves them (maybe because it's an external table)
        // use the class since StructType requires it ...
        columnNames = new ArrayList<String>(Arrays.asList(StringUtils.split(tbl.getProperty("columns"), ",")));
        List<TypeInfo> colTypes = TypeInfoUtils.getTypeInfosFromTypeString(tbl.getProperty("columns.types"));

        // create a standard Object Inspector - note we're not using it for serialization/deserialization
        List<ObjectInspector> inspectors = new ArrayList<ObjectInspector>();

        for (TypeInfo typeInfo : colTypes) {
            inspectors.add(TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(typeInfo));
        }

        inspector = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, inspectors);
    }

    @Override
    public Object deserialize(Writable blob) throws SerDeException {
        if (blob == null || blob instanceof NullWritable) {
            return null;
        }

        StructTypeInfo structTypeInfo = (StructTypeInfo) TypeInfoUtils.getTypeInfoFromObjectInspector(inspector);
        return hiveFromWritable(structTypeInfo, blob);
    }

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return inspector;
    }

    @Override
    public SerDeStats getSerDeStats() {
        // TODO: stats not yet supported (seems quite the trend for SerDe)
        return null;
    }

    @Override
    public Class<? extends Writable> getSerializedClass() {
        return MapWritable.class;
    }

    @Override
    // TODO: maybe reuse object?
    public Writable serialize(Object data, ObjectInspector objInspector) throws SerDeException {
        //overwrite field names (as they get lost by Hive)
        StructTypeInfo structTypeInfo = (StructTypeInfo) TypeInfoUtils.getTypeInfoFromObjectInspector(objInspector);
        structTypeInfo.setAllStructFieldNames(columnNames);

        return hiveToWritable(structTypeInfo, data);
    }

    @SuppressWarnings("unchecked")
    static Writable hiveToWritable(TypeInfo type, Object data) {
        if (data == null) {
            return NullWritable.get();
        }

        switch (type.getCategory()) {
        case PRIMITIVE:
            // handle lazy objects differently as the Lazy ObjectInspector breaks the generic contract for #getPrimitive
            if (data instanceof LazyPrimitive) {
                return ((LazyPrimitive) data).getWritableObject();
            }
            // use standard
            return (Writable) ((PrimitiveObjectInspector) TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(type)).getPrimitiveWritableObject(data);
        case LIST: // or ARRAY
            ListTypeInfo listType = (ListTypeInfo) type;
            TypeInfo listElementType = listType.getListElementTypeInfo();

            List<Object> list = null;
            Writable[] arrayContent = null;

            if (data instanceof LazyArray) {
                list = ((LazyArray) data).getList();
            }
            else {
                if (data.getClass().isArray()) {
                    data = Arrays.asList((Object[]) data);
                }
                list = (List<Object>) data;
            }

            if (!list.isEmpty()) {
                arrayContent = new Writable[list.size()];
                for (int i = 0; i < arrayContent.length; i++) {
                    arrayContent[i] = hiveToWritable(listElementType, list.get(i));
                }
            }

            return (arrayContent != null ? new ArrayWritable(arrayContent[0].getClass(), arrayContent) : new ArrayWritable(NullWritable.class, new Writable[0]));

        case MAP:
            MapTypeInfo mapType = (MapTypeInfo) type;

            MapWritable map = new MapWritable();

            Map<Object, Object> mapContent = null;

            if (data instanceof LazyMap) {
                mapContent = ((LazyMap) data).getMap();
            }
            else {
                mapContent = (Map<Object, Object>) data;
            }

            for (Map.Entry<Object, Object> entry : mapContent.entrySet()) {
                map.put(hiveToWritable(mapType.getMapKeyTypeInfo(), entry.getKey()),
                        hiveToWritable(mapType.getMapValueTypeInfo(), entry.getValue()));
            }

            return map;
            //break;
        case STRUCT:
            StructTypeInfo structType = (StructTypeInfo) type;
            List<TypeInfo> info = structType.getAllStructFieldTypeInfos();
            List<String> names = structType.getAllStructFieldNames();
            List<Object> content = null;

            map = new MapWritable();

            if (data instanceof LazyStruct) {
                content = ((LazyStruct) data).getFieldsAsList();
            }
            else {
                // shortcut in getting struct content
                content = (data instanceof List ? (List<Object>) data : Arrays.asList((Object[]) data));
            }

            for (int structIndex = 0; structIndex < info.size(); structIndex++) {
                map.put(new Text(names.get(structIndex)),
                        hiveToWritable(info.get(structIndex), content.get(structIndex)));
            }
            return map;
        case UNION:
            UnionTypeInfo unionType = (UnionTypeInfo) type;

            throw new UnsupportedOperationException("union not yet supported");//break;


        default:
            //log.warn("Unknown type " + column.type.getTypeName() + " for column " + column.name + "; using toString()");
            return new Text(data.toString());
        }
    }

    static Object hiveFromWritable(TypeInfo type, Writable data) {
        if (data == null || data instanceof NullWritable) {
            return null;
        }

        switch (type.getCategory()) {
        case LIST: {// or ARRAY
            ListTypeInfo listType = (ListTypeInfo) type;
            TypeInfo listElementType = listType.getListElementTypeInfo();

            ArrayWritable aw = (ArrayWritable) data;

            List<Object> list = new ArrayList<Object>();
            for (Writable writable : aw.get()) {
                list.add(hiveFromWritable(listElementType, writable));
            }

            return list;
        }
        case STRUCT: {
            StructTypeInfo structType = (StructTypeInfo) type;
            List<String> names = structType.getAllStructFieldNames();
            List<TypeInfo> info = structType.getAllStructFieldTypeInfos();

            // return just the values
            List<Object> struct = new ArrayList<Object>();

            MapWritable map = (MapWritable) data;
            Text reuse = new Text();
            for (int index = 0; index < names.size(); index++) {
                reuse.set(names.get(index));
                struct.add(hiveFromWritable(info.get(index), map.get(reuse)));
            }
            return struct;
        }

        case UNION: {
            throw new UnsupportedOperationException("union not yet supported");//break;
        }
        }
        // return as is
        return data;
    }
}