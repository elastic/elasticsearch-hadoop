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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.elasticsearch.hadoop.util.StringUtils;

/**
 *
 */
abstract class PigTypeUtils {

    private static final Log log = LogFactory.getLog(ESStorage.class);

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