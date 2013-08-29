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

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

/**
 * Wrapper class around a Hive type - the value and its associated schema.
 */
public class HiveType {

    private Object object;
    private ObjectInspector oi;

    public HiveType(Object object, ObjectInspector info) {
        this.object = object;
        this.oi = info;
    }

    public Object getObject() {
        return object;
    }

    public void setObject(Object object) {
        this.object = object;
    }

    public ObjectInspector getObjectInspector() {
        return oi;
    }

    public void setObjectInspector(ObjectInspector oi) {
        this.oi = oi;
    }
}
