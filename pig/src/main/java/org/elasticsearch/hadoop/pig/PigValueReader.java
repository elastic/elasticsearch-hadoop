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
package org.elasticsearch.hadoop.pig;

import java.util.List;

import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.TupleFactory;
import org.elasticsearch.hadoop.serialization.builder.JdkValueReader;

public class PigValueReader extends JdkValueReader {

    @Override
    public Object addToArray(Object array, List<Object> value) {
        return TupleFactory.getInstance().newTupleNoCopy(value);
    }

    @Override
    protected Object binaryValue(byte[] value) {
        return new DataByteArray(value);
    }

    @Override
    protected Object date(String value) {
        return PigUtils.convertDateFromES(value);
    }
}