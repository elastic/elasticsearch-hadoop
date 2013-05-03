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
package org.elasticsearch.hadoop.pig;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 *
 */
abstract class PigTypeUtils {

    private static final Log log = LogFactory.getLog(ESStorage.class);

    @SuppressWarnings("unchecked")
    static Object pigToObject(Object object, ResourceFieldSchema field) {
        switch (field.getType()) {
        case DataType.NULL:
            return null;
        case DataType.BOOLEAN:
        case DataType.INTEGER:
        case DataType.LONG:
        case DataType.FLOAT:
        case DataType.DOUBLE:
        case DataType.CHARARRAY:
            return object;
        case DataType.BYTEARRAY:
            return ((DataByteArray)object).get();

        case DataType.MAP:
            ResourceSchema nestedSchema = field.getSchema();
            ResourceFieldSchema[] nestedFields = nestedSchema.getFields();

            Map<String, Object> map = new LinkedHashMap<String, Object>();
            int index = 0;
            for (Map.Entry<String, Object> entry : ((Map<String, Object>) object).entrySet()) {
                map.put(entry.getKey(), pigToObject(entry.getValue(), nestedFields[index++]));
            }
            return map;

        case DataType.TUPLE:
            nestedSchema = field.getSchema();
            nestedFields = nestedSchema.getFields();
            map = new LinkedHashMap<String, Object>();

            // use getAll instead of get(int) to avoid having to handle Exception...
            List<Object> tuples = ((Tuple) object).getAll();
            for (int i = 0; i < nestedFields.length; i++) {
                String name = nestedFields[i].getName();
                // handle schemas without names
                name = (StringUtils.isBlank(name) ? Integer.toString(i) : name);
                map.put(name, pigToObject(tuples.get(i), nestedFields[i]));
            }
            return map;

        case DataType.BAG:
            nestedSchema = field.getSchema();
            ResourceFieldSchema bagType = nestedSchema.getFields()[0];
            List<Object> bag = new ArrayList<Object>();

            for (Tuple tuple : (DataBag) object) {
                bag.add(pigToObject(tuple, bagType));
            }
            return bag;

        default:
            log.warn("Unknown type " + DataType.findTypeName(field.getType()) + "| using toString()");
            return object.toString();
        }
    }

    static Object objectToPig(Object object) {
        if (object == null) {
            return null;
        }
        if (object instanceof Boolean || object instanceof Number || object instanceof String || object instanceof Character) {
            return object;
        }
        if (object instanceof byte[]) {
            return new DataByteArray((byte[])object);
        }
        // TODO: for now just consider it a Tuple
        if (object instanceof List) {
            List<Object> list = (List<Object>) object;
            List<Object> converted = new ArrayList<Object>(list.size());
            for (Object obj : list) {
                converted.add(objectToPig(obj));
            }
            return TupleFactory.getInstance().newTupleNoCopy(converted);
        }
        if (object instanceof Map) {
            Map<Object, Object> map = (Map<Object, Object>) object;

            Map<Object, Object> data = new LinkedHashMap<Object, Object>(map.size());
            for (Entry<Object, Object> entry: map.entrySet()) {
                data.put(entry.getKey(), PigTypeUtils.objectToPig(entry.getValue()));
            }
            return data;
        }

        log.warn("Unknown type " + DataType.findTypeName(object) + "| using toString()");
        return object.toString();
    }
}